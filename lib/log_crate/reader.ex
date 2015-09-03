defmodule LogCrate.Reader do
  alias __MODULE__
  use GenServer

  defstruct reply_to:   nil,
            dir:        nil,
            segment_id: nil,
            entries:    nil,
            batch:      nil

  @spec start_link(pid, binary, integer, [IndexEntry.t], boolean) :: GenServer.on_start
  def start_link(reply_to, dir, segment_id, entries, batch) do
    GenServer.start_link(__MODULE__, {reply_to, dir, segment_id, entries, batch})
  end

  #
  # GenServer callbacks
  #

  def init({reply_to, dir, segment_id, entries, batch}) do
    reader = %Reader{
      reply_to:   reply_to,
      dir:        dir,
      segment_id: segment_id,
      entries:    entries,
      batch:      batch,
    }
    {:ok, reader, 0}
  end

  def handle_info(:timeout, reader) do
    basename = :io_lib.format("~16.16.0b.dat", [reader.segment_id]) |> IO.iodata_to_binary
    path = "#{reader.dir}/#{basename}"

    data_list = Enum.map(reader.entries, fn(entry) ->
      # skip past the header
      pos = entry.pos + 4
      size = entry.size - 4

      # read
      {:ok, io} = File.open(path)
      {:ok, data} = :file.pread(io, pos, size)

      data
    end)

    # report result
    if reader.batch do
      GenServer.cast(reader.reply_to, {:read_result, reader.segment_id, data_list})
    else
      GenServer.reply(reader.reply_to, data_list |> List.first)
    end
    {:stop, :normal, reader}
  end
end
