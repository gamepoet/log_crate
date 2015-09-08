defmodule LogCrate do
  @moduledoc """
  LogCrate is an append-only log-structured storage module that persists to
  disk. Records appended to the crate are each given an sequentially increasing
  id that can be used to later retrieve that record.

  LogCrate manages a directory on disk that contains files containing the
  records persisted to disk. Records are appended to the "current" segment file
  until it reached a maximum size at which point a new segment file is rolled.
  """
  alias __MODULE__
  alias __MODULE__.BatchReader
  alias __MODULE__.Config
  alias __MODULE__.IndexEntry
  alias __MODULE__.Reader
  alias __MODULE__.RecordHeader
  alias __MODULE__.Writer
  require Logger
  use GenServer

  @type record_id :: integer
  @type digest :: binary
  @type value :: binary

  @opaque t :: %__MODULE__{
    config:         Config.t,
  }
  defstruct config:            nil,
            index:             nil,
            in_flight_appends: nil,
            writer:            nil

  @doc """
  Creates a new, empty log crate at the given directory. This spawns a GenServer
  to manage that crate and links it to the current process.

  Options:
    * `:segment_max_size` - the maximum byte size a segment is allowed to grow
      to before a new segment is rolled

  Returns:
    * `crate` - the pid of the newly created crate process
    * `{:error, :directory_exists}` - creation failed because the destination directory already exists
  """
  @spec create(binary, Keyword.t) :: pid | GenServer.on_start | {:error, :directory_exists}
  def create(dir, opts \\ []) do
    if File.exists?(dir) do
      {:error, :directory_exists}
    else
      config = Config.new(dir, opts)
      case GenServer.start_link(__MODULE__, {:create, config}) do
        {:ok, pid} ->
          pid
        other ->
          other
      end
    end
  end

  @doc """
  Opens an existing log crate at the given directory. This spawns a GenServer to
  manages that crate and links it to the current process.

  Options:
    * `:segment_max_size` - the maximum byte size a segment is allowed to grow
      to before a new segment is rolled

  Returns:
    * `crate` - the pid of the newly created crate process
    * `{:error, :directory_exists}` - creation failed because the destination directory already exists
  """
  @spec open(binary, Keyword.t) :: pid | GenServer.on_start | {:error, :directory_missing}
  def open(dir, opts \\ []) do
    if File.exists?(dir) do
      config = Config.new(dir, opts)
      case GenServer.start_link(__MODULE__, {:open, config}) do
        {:ok, pid} ->
          pid
        other ->
          other
      end
    else
      {:error, :directory_missing}
    end
  end

  @doc """
  Closes the given crate and waits for it to close.

  Returns:
    * `:ok`
  """
  @spec close(pid) :: :ok
  def close(crate_pid) do
    GenServer.call(crate_pid, :close)
  end

  @doc """
  Tests if the given crate is empty.

  Returns:
    * true if the crate is empty
    * false otherwise
  """
  @spec empty?(pid) :: boolean
  def empty?(crate_pid) do
    GenServer.call(crate_pid, :empty?)
  end

  @doc """
  Appends the given value or values to the crate and assigns them record ids.

  Returns:
    * `record_id` - the id assigned to the record if successful (non-list form)
    * `[record_id]` - the ids assigned to the records if successful (list form)
    * `{:error, reason}` - appending was unsuccessful
  """
  @spec append(pid, [{digest, value}] | {digest, value}) :: [record_id] | record_id | {:error, any}
  def append(crate_pid, values) when is_list(values) do
    GenServer.call(crate_pid, {:append, values})
  end
  def append(crate_pid, {digest, data} = value) when is_binary(digest) and is_binary(data) do
    case append(crate_pid, [value]) do
      {:error, _} = err ->
        err
      [record_id] ->
        record_id
    end
  end

  @doc """
  Retrieves the value of the record stored with the given record id.

  Returns:
    * `{digest, value}` - the digest and value stored in the record if successful
    * `:not_found` - there is no record for the requested id in the crate
    * `{:error, reason}` - the read was unsuccessful
  """
  @spec read(pid, record_id) :: {digest, value} | :not_found | {:error, any}
  def read(crate_pid, record_id) do
    GenServer.call(crate_pid, {:read, record_id})
  end

  @doc """
  Retrieves the values of records stored started with the given starting record
  id. All records up through the max byte size or the end of the crate will be
  retrieved.

  Returns:
    * `[]` - the starting record id is beyond the end of the crate
    * `[{digest, value}]` - the digests and values stored in the records if successful
    * `{:error, reason}` - the read was unsuccessful
  """
  @spec read(pid, record_id, integer) :: [{digest, value}] | {:error, any}
  def read(crate_pid, record_id_start, max_byte_size) do
    GenServer.call(crate_pid, {:read, record_id_start, max_byte_size})
  end

  @doc """
  Retrieves the range of record ids for the records stored in the crate. This
  may not be 0..N if old segments have been deleted from the crate.

  Returns:
    * `nil` - the crate is empty
    * `first..last` - the range of record ids
  """
  @spec range(pid) :: nil | Range.t
  def range(crate_pid) do
    GenServer.call(crate_pid, :range)
  end

  #
  # GenServer callbacks
  #

  def init({init_mode, config}) do
    crate = %LogCrate{
      config:            config,
      index:             HashDict.new,
      in_flight_appends: :queue.new,
    }
    {:ok, {init_mode, crate}, 0}
  end

  def terminate(_reason, _state) do
    :ok
  end

  # initialization that creates a new crate
  def handle_info(:timeout, {:create, %LogCrate{} = crate}) do
    File.mkdir_p!(crate.config.dir)
    {:ok, writer} = Writer.start_link(self, crate.config, :create)
    crate = %{crate | writer: writer}
    {:noreply, crate}
  end

  # initialization that opens an existing crate
  def handle_info(:timeout, {:open, %LogCrate{} = crate}) do
    # scan the directory for existing segments
    {:ok, files} = File.ls(crate.config.dir)
    files = Enum.sort(files)
    {index, final_segment_id, final_record_id} = Enum.reduce(files, {crate.index, nil, nil}, fn(file, {index, _final_segment_id, _final_record_id}) ->
      load_segment(index, Path.join(crate.config.dir, file))
    end)
    next_record_id = final_record_id + 1

    {:ok, writer} = Writer.start_link(self, crate.config, :open, final_segment_id, next_record_id)
    crate = %{crate | index: index, writer: writer}
    {:noreply, crate}
  end

  def handle_call(:close, _from, %LogCrate{} = crate) do
    :ok = Writer.close(crate.writer)
    {:stop, :normal, :ok, crate}
  end

  def handle_call(:empty?, _from, %LogCrate{} = crate) do
    result = true
    {:reply, result, crate}
  end

  def handle_call({:append, values}, from, %LogCrate{} = crate) when is_list(values) do
    Writer.append(crate.writer, values)
    new_queue = :queue.in(from, crate.in_flight_appends)
    crate = %{crate | in_flight_appends: new_queue}

    {:noreply, crate}
  end

  def handle_call({:read, record_id}, from, %LogCrate{} = crate) do
    case Dict.get(crate.index, record_id) do
      nil ->
        GenServer.reply(from, :not_found)

      entry ->
        Reader.start_link(from, crate.config.dir, entry.segment_id, [entry], false)
    end

    {:noreply, crate}
  end

  def handle_call({:read, record_id_start, max_byte_size}, from, %LogCrate{} = crate) do
    case Dict.get(crate.index, record_id_start) do
      nil ->
        # starting record doesn't exist, so bail
        GenServer.reply(from, :not_found)

      _ ->
        # get the reading list
        case get_read_list(crate.index, record_id_start, max_byte_size) do
          [] ->
            GenServer.reply(from, [])
          read_list ->
            BatchReader.start_link(from, crate.config.dir, read_list)
        end
    end

    {:noreply, crate}
  end

  def handle_call(:range, _from, %LogCrate{} = crate) do
    record_ids = Dict.keys(crate.index)
    if Enum.empty?(record_ids) do
      range = nil
    else
      range = Enum.min(record_ids)..Enum.max(record_ids)
    end
    {:reply, range, crate}
  end


  # event from the Writer when a record has been committed to disk
  def handle_cast({:did_append, segment_id, record_ids, positions, sizes, digests}, %LogCrate{} = crate) do
    crate = case :queue.out(crate.in_flight_appends) do
      {{:value, caller}, new_queue} ->
        # notify the caller
        GenServer.reply(caller, record_ids)

        # update the index
        metadata = List.zip([record_ids, positions, sizes, digests])
        new_index = Enum.reduce(metadata, crate.index, fn({record_id, fpos, size, digest}, index) ->
          Dict.put(index, record_id, IndexEntry.new(segment_id, fpos, size, digest))
        end)
        %{crate | in_flight_appends: new_queue, index: new_index}

      {:empty, _new_queue} ->
        Logger.error("BUG LogCrate got commit from writer but in_flight_appends queue is empty. segment_id=#{segment_id} record_ids=#{inspect record_ids} positions=#{inspect positions} sizes=#{inspect sizes} digests=#{inspect digests}")
        throw(:bug)
    end

    {:noreply, crate}
  end

  # event from the Writer when there was a problem comitting to disk
  def handle_cast({:error_append, record_id, reason}, %LogCrate{} = crate) do
    crate = case :queue.out(crate.in_flight_appends) do
      {{:value, caller}, new_queue} ->
        # notify the caller
        GenServer.reply(caller, {:error, reason})
        %{crate | in_flight_appends: new_queue}

      {:empty, _new_queue} ->
        Logger.error("BUG LogCrate got error from writer but in_flight_appends queue is empty. record_id=#{inspect record_id}")
        throw(:bug)
    end

    {:noreply, crate}
  end

  # event from the Writer when it has rolled over to a new segment
  def handle_cast({:did_roll, _new_segment_id}, %LogCrate{} = crate) do
    {:noreply, crate}
  end

  # gets a list of records to read that satifies the given read request
  defp get_read_list(index, record_id_start, max_bytes) do
    get_read_list(index, record_id_start, max_bytes, [])
  end
  defp get_read_list(index, record_id_start, max_bytes, matches) do
    case Dict.get(index, record_id_start) do
      nil ->
        # no more records in the crate
        matches |> Enum.reverse

      entry ->
        # is the record small enough to satisfy max bytes?
        size = entry.size - RecordHeader.size
        if size > max_bytes do
          # record is too big
          matches |> Enum.reverse
        else
          get_read_list(index, record_id_start + 1, max_bytes - size, [entry | matches])
        end
    end
  end

  defp load_segment(index, filename) do
    {:ok, io} = File.open(filename, [:read])
    file_header = IO.binread(io, 20)
    <<
      magic       ::binary-size(8),
      version     ::integer-size(32),
      segment_id  ::integer-size(64),
    >> = file_header

    # verify the header
    "logcrate" = magic
    1          = version

    record_id = segment_id
    {index, final_record_id} = case load_record(index, io, segment_id, record_id) do
      {:error, _} = err ->
        err
      result ->
        result
    end

    :ok = File.close(io)
    {index, segment_id, final_record_id}
  end

  defp load_record(index, io, segment_id, record_id) do
    {:ok, pos} = :file.position(io, :cur)
    case read_record_header(io) do
      :eof ->
        {index, record_id - 1}
      {:error, _} = err ->
        err
      {size, digest} ->
        case read_record_content(io, size) do
          {:error, _} = err ->
            err
          _record_content ->
            total_size = RecordHeader.size + size
            index = Dict.put(index, record_id, IndexEntry.new(segment_id, pos, total_size, digest))
            load_record(index, io, segment_id, record_id + 1)
        end
    end
  end

  defp read_record_header(io) do
    case IO.binread(io, RecordHeader.size) do
      :eof ->
        :eof
      {:error, _} = err ->
        err
      header ->
        {size, digest, ""} = RecordHeader.decode(header)
        {size, digest}
    end
  end

  defp read_record_content(io, record_size) do
    case IO.binread(io, record_size) do
      {:error, _} = err ->
        err
      :eof ->
        {:error, {:corrupt, :eof}}
      data ->
        data
    end
  end
end
