defmodule LogCrate.Writer do
  @moduledoc """
  Manages the write IO for a crate segment. This process is responsible for
  serializing the mutation ops.
  """
  alias __MODULE__
  alias LogCrate.Config
  require Logger
  use GenServer

  @opaque t :: %__MODULE__{}
  defstruct crate_pid:      nil,
            config:         nil,
            segment_id:     nil,
            next_record_id: nil,
            pos:            nil,
            io:             nil

  @spec start_link(pid, Config.t, :create) :: GenServer.on_start
  def start_link(crate_pid, config, :create) do
    GenServer.start_link(__MODULE__, {crate_pid, config, :create})
  end

  @spec start_link(pid, Config.t, :open, integer, integer) :: GenServer.on_start
  def start_link(crate_pid, config, :open, segment_id, next_record_id) do
    GenServer.start_link(__MODULE__, {crate_pid, config, :open, segment_id, next_record_id})
  end

  @spec append(pid, [binary]) :: :ok
  def append(writer_pid, values) do
    GenServer.cast(writer_pid, {:append, values})
  end

  @spec close(pid) :: :ok
  def close(writer_pid) do
    GenServer.call(writer_pid, :close)
  end


  #
  # GenServer callbacks
  #

  def init({crate_pid, config, :create}) do
    writer = %Writer{
      crate_pid:      crate_pid,
      config:         config,
      segment_id:     0,
      next_record_id: 0,
      pos:            0,
      io:             nil,
    }
    {:ok, {writer, :create}, 0}
  end

  def init({crate_pid, config, :open, segment_id, next_record_id}) do
    writer = %Writer{
      crate_pid:      crate_pid,
      config:         config,
      segment_id:     segment_id,
      next_record_id: next_record_id,
      pos:            0,
      io:             nil,
    }
    {:ok, {writer, :open}, 0}
  end

  def terminate(_reason, writer) do
    unless is_nil(writer.io) do
      File.close(writer.io)
    end
    :ok
  end

  def handle_info(:timeout, {%Writer{} = writer, :create}) do
    {:noreply, writer}
  end

  def handle_info(:timeout, {%Writer{} = writer, :open}) do
    {:ok, io} = File.open(segment_filename(writer.config.dir, writer.segment_id), [:read, :write])
    {:ok, pos} = :file.position(io, :eof)
    writer = %{writer | io: io, pos: pos}
    {:noreply, writer}
  end

  def handle_cast({:append, values}, %Writer{} = writer) do
    # reserve the record ids
    record_id = writer.next_record_id
    record_count = length(values)
    next_record_id = record_id + record_count

    # prepare the records for writing
    records = Enum.map(values, fn(value) ->
      header = <<byte_size(value)::size(32)>>
      [header, value]
    end)
    write_size = IO.iodata_length(records)

    # compute record sizes
    record_sizes = Enum.map(records, fn(record) ->
      IO.iodata_length(record)
    end)

    # roll a new segment if needed
    writer = maybe_roll(writer, record_id, write_size)
    fpos_start = writer.pos

    # derive file positions
    {fpos_list, _} = Enum.reduce(record_sizes, {[], fpos_start}, fn(size, {accum, pos}) ->
      {[pos | accum], pos + size}
    end)
    fpos_list = Enum.reverse(fpos_list)

    # build the record ids
    last_record_id = record_id + record_count - 1
    record_ids = Enum.to_list(record_id..last_record_id)

    # write the record to disk
    writer = case IO.binwrite(writer.io, records) do
      :ok ->
        notify(writer, {:did_append, writer.segment_id, record_ids, fpos_list, record_sizes})
        %{writer | pos: writer.pos + write_size, next_record_id: next_record_id}

      {:error, reason} ->
        Logger.error "Failed to write value #{inspect reason}"
        notify(writer, {:error_append, record_id, reason})
        writer
    end
    {:noreply, writer}
  end

  def handle_call(:close, _from, %Writer{} = writer) do
    {:stop, :normal, :ok, writer}
  end


  defp maybe_roll(%Writer{io: nil} = writer, record_id, _size) do
    roll(writer, record_id)
  end
  defp maybe_roll(writer, record_id, size) do
    if writer.pos + size > writer.config.segment_max_size do
      roll(writer, record_id)
    else
      writer
    end
  end

  defp roll(writer, record_id) do
    unless is_nil(writer.io) do
      :ok = File.close(writer.io)
    end

    segment_id = record_id
    {:ok, io} = File.open(segment_filename(writer.config.dir, segment_id), [:write])
    header = file_header(segment_id)
    :ok = IO.binwrite(io, header)

    notify(writer, {:did_roll, segment_id})
    %{writer | io: io, segment_id: segment_id, pos: byte_size(header)}
  end

  defp notify(writer, evt) do
    GenServer.cast(writer.crate_pid, evt)
  end

  defp segment_filename(dir, segment_id) do
    basename = :io_lib.format("~16.16.0b.dat", [segment_id]) |> IO.iodata_to_binary
    "#{dir}/#{basename}"
  end

  defp file_header(segment_id) do
    <<
      "logcrate"    ::binary,           # magic
      1             ::integer-size(32), # version
      segment_id    ::integer-size(64), # segment id
    >>
  end

end
