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
  defstruct crate_pid:  nil,
            config:     nil,
            segment_id: nil,
            pos:        nil,
            io:         nil

  @spec start_link(pid, Config.t, :create | :open, integer) :: GenServer.on_start
  def start_link(crate_pid, config, mode, segment_id) do
    GenServer.start_link(__MODULE__, {crate_pid, config, mode, segment_id})
  end

  @spec append(pid, integer, binary) :: :ok
  def append(writer_pid, record_id, value) do
    GenServer.cast(writer_pid, {:append, record_id, value})
  end

  @spec close(pid) :: :ok
  def close(writer_pid) do
    GenServer.call(writer_pid, :close)
  end


  #
  # GenServer callbacks
  #

  def init({crate_pid, config, mode, segment_id}) do
    writer = %Writer{
      crate_pid:  crate_pid,
      config:     config,
      segment_id: segment_id,
      pos:        0,
      io:         nil,
    }
    {:ok, {writer, mode, segment_id}, 0}
  end

  def terminate(_reason, writer) do
    unless is_nil(writer.io) do
      File.close(writer.io)
    end
    :ok
  end

  def handle_info(:timeout, {%Writer{} = writer, :create, 0}) do
    {:noreply, writer}
  end

  def handle_info(:timeout, {%Writer{} = writer, :open, segment_id}) do
    {:ok, io} = File.open(segment_filename(writer.config.dir, segment_id), [:read, :write])
    {:ok, pos} = :file.position(io, :eof)
    writer = %{writer | io: io, pos: pos}
    {:noreply, writer}
  end

  def handle_cast({:append, record_id, value}, %Writer{} = writer) do
    # prepare the record
    header = <<byte_size(value)::size(32)>>
    data = [header, value]
    data_size = byte_size(header) + byte_size(value)

    # roll a new segment if needed
    writer = maybe_roll(writer, record_id, data_size)

    # figure out where we are in the file
    fpos = writer.pos

    # write the record to disk
    writer = case IO.binwrite(writer.io, data) do
      :ok ->
        notify(writer, {:did_append, writer.segment_id, record_id, fpos, data_size})
        %{writer | pos: writer.pos + data_size}

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
