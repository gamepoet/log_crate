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
  defstruct crate_pid: nil,
            config:    nil,
            pos:       nil,
            io:        nil

  @spec start_link(pid, Config.t) :: GenServer.on_start
  def start_link(crate_pid, config) do
    GenServer.start_link(__MODULE__, {crate_pid, config})
  end

  @spec append(pid, integer, binary) :: :ok
  def append(writer_pid, msg_id, value) do
    GenServer.cast(writer_pid, {:append, msg_id, value})
  end


  #
  # GenServer callbacks
  #

  def init({crate_pid, config}) do
    writer = %Writer{
      crate_pid: crate_pid,
      config:    config,
      pos:       0,
      io:        nil,
    }
    {:ok, writer}
  end

  def handle_cast({:append, msg_id, value}, %Writer{} = writer) do
    # prepare the record
    header = <<byte_size(value)::size(32)>>
    data = [header, value]
    data_size = byte_size(header) + byte_size(value)

    # roll a new segment if needed
    writer = maybe_roll(writer, msg_id, data_size)

    # figure out where we are in the file
    fpos = writer.pos

    # write the record to disk
    writer = case IO.binwrite(writer.io, data) do
      :ok ->
        notify(writer, {:commit, msg_id, fpos, data_size})
        %{writer | pos: writer.pos + data_size}

      {:error, reason} ->
        Logger.error "Failed to write value #{inspect reason}"
        notify(writer, {:error_append, msg_id, reason})
        writer
    end
    {:noreply, writer}
  end


  defp maybe_roll(%Writer{io: nil} = writer, msg_id, _size) do
    roll(writer, msg_id)
  end
  defp maybe_roll(writer, _msg_id, _size) do
    # TODO: support rolling over to new segments
    writer
  end

  defp roll(writer, msg_id) do
    unless is_nil(writer.io) do
      :ok = File.close(writer.io)
    end

    basename = :io_lib.format("~16.16.0b.dat", [msg_id]) |> IO.iodata_to_binary
    path = "#{writer.config.dir}/#{basename}"
    {:ok, io} = File.open(path, [:write])

    notify(writer, {:roll, msg_id})
    %{writer | io: io, pos: 0}
  end

  defp notify(writer, evt) do
    GenServer.cast(writer.crate_pid, evt)
  end
end
