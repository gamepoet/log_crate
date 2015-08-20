defmodule LogCrate do
  alias __MODULE__
  alias __MODULE__.Config
  alias __MODULE__.IndexEntry
  alias __MODULE__.Reader
  alias __MODULE__.Writer
  require Logger
  use GenServer

  @type msg_id :: integer
  @type value :: binary

  @opaque t :: %__MODULE__{
    config:      Config.t,
    next_msg_id: integer,
  }
  defstruct config:            nil,
            index:             nil,
            next_msg_id:       nil,
            in_flight_appends: nil,
            writer:            nil

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

  @spec close(pid) :: :ok
  def close(crate_pid) do
    GenServer.call(crate_pid, :close)
  end

  @spec empty?(pid) :: boolean
  def empty?(crate_pid) do
    GenServer.call(crate_pid, :empty?)
  end

  @spec append(pid, value | [value]) :: msg_id | {:error, any}
  def append(crate_pid, value) do
    GenServer.call(crate_pid, {:append, value})
  end

  @spec read(pid, msg_id) :: value | :not_found | {:error, any}
  def read(crate_pid, msg_id) do
    GenServer.call(crate_pid, {:read, msg_id})
  end


  #
  # GenServer callbacks
  #

  def init({init_mode, config}) do
    crate = %LogCrate{
      config:            config,
      index:             HashDict.new,
      next_msg_id:       0,
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
    {:ok, writer} = Writer.start_link(self, crate.config, :create, 0)
    crate = %{crate | writer: writer}
    {:noreply, crate}
  end

  # initialization that opens an existing crate
  def handle_info(:timeout, {:open, %LogCrate{} = crate}) do
    # scan the directory for existing segments
    {:ok, files} = File.ls(crate.config.dir)
    files = Enum.sort(files)
    {index, final_segment_id} = Enum.reduce(files, {crate.index, nil}, fn(file, {index, _final_segment_id}) ->
      load_segment(index, Path.join(crate.config.dir, file))
    end)

    {:ok, writer} = Writer.start_link(self, crate.config, :open, final_segment_id)
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

  def handle_call({:append, value}, from, %LogCrate{} = crate) do
    msg_id = crate.next_msg_id
    Writer.append(crate.writer, msg_id, value)
    new_queue = :queue.in(from, crate.in_flight_appends)
    crate = %{crate | next_msg_id: crate.next_msg_id + 1, in_flight_appends: new_queue}

    {:noreply, crate}
  end

  def handle_call({:read, msg_id}, from, %LogCrate{} = crate) do
    case Dict.get(crate.index, msg_id) do
      nil ->
        GenServer.reply(from, :not_found)

      entry ->
        Reader.start_link(from, crate.config.dir, entry.segment_id, entry.pos, entry.size)
    end

    {:noreply, crate}
  end


  # event from the Writer when a message has been committed to disk
  def handle_cast({:did_append, segment_id, msg_id, pos, size}, %LogCrate{} = crate) do
    crate = case :queue.out(crate.in_flight_appends) do
      {{:value, caller}, new_queue} ->
        GenServer.reply(caller, msg_id)
        new_index = Dict.put(crate.index, msg_id, IndexEntry.new(segment_id, pos, size))
        %{crate | in_flight_appends: new_queue, index: new_index}

      {:empty, _new_queue} ->
        Logger.error("BUG LogCrate got commit from writer but in_flight_appends queue is empty. segment_id=#{segment_id} msg_id=#{msg_id} pos=#{pos} size=#{size}")
        throw(:bug)
    end

    {:noreply, crate}
  end

  # event from the Writer when it has rolled over to a new segment
  def handle_cast({:did_roll, _new_segment_id}, %LogCrate{} = crate) do
    {:noreply, crate}
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

    msg_id = segment_id
    index = case load_message(index, io, segment_id, msg_id) do
      {:error, _} = err ->
        err
      index ->
        index
    end

    :ok = File.close(io)
    {index, segment_id}
  end

  def load_message(index, io, segment_id, msg_id) do
    {:ok, pos} = :file.position(io, :cur)
    case read_message_size(io) do
      {:error, {:corrupt, :eof}} ->
        index
      {:error, _} = err ->
        err
      msg_size ->
        case read_message_content(io, msg_size) do
          {:error, _} = err ->
            err
          _msg_content ->
            index = Dict.put(index, msg_id, IndexEntry.new(segment_id, pos, msg_size + 4))
            load_message(index, io, segment_id, msg_id + 1)
        end
    end
  end

  def read_message_size(io) do
    case IO.binread(io, 4) do
      {:error, _} = err ->
        err
      :eof ->
        {:error, {:corrupt, :eof}}
      <<msg_size::integer-size(32)>> ->
        msg_size
    end
  end

  def read_message_content(io, msg_size) do
    case IO.binread(io, msg_size) do
      {:error, _} = err ->
        err
      :eof ->
        {:error, {:corrupt, :eof}}
      data ->
        data
    end
  end
end
