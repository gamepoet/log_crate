defmodule LogCrate.Reader do
  alias __MODULE__
  use GenServer

  defstruct reply_to:   nil,
            dir:        nil,
            segment_id: nil,
            pos:        nil,
            size:       nil

  @spec start_link(pid, binary, integer, integer, integer) :: GenServer.on_start
  def start_link(reply_to, dir, segment_id, pos, size) do
    GenServer.start_link(__MODULE__, {reply_to, dir, segment_id, pos, size})
  end

  #
  # GenServer callbacks
  #

  def init({reply_to, dir, segment_id, pos, size}) do
    reader = %Reader{
      reply_to: reply_to,
      dir:        dir,
      segment_id: segment_id,
      pos:        pos,
      size:       size,
    }
    {:ok, reader, 0}
  end

  def handle_info(:timeout, reader) do
    basename = :io_lib.format("~16.16.0b.dat", [reader.segment_id]) |> IO.iodata_to_binary
    path = "#{reader.dir}/#{basename}"

    # skip past the header
    pos = reader.pos + 4
    size = reader.size - 4

    # read
    {:ok, io} = File.open(path)
    {:ok, data} = :file.pread(io, pos, size)

    # report result
    GenServer.reply(reader.reply_to, data)
    {:stop, :normal, reader}
  end
end
