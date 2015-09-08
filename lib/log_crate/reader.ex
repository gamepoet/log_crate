defmodule LogCrate.Reader do
  alias __MODULE__
  alias LogCrate.RecordHeader
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

    values = Enum.map(reader.entries, fn(entry) ->
      # read the record from the file
      {:ok, data} = File.open(path, [:read], fn(io) ->
        {:ok, data} = :file.pread(io, entry.pos, entry.size)
        data
      end)

      # split up the content
      case RecordHeader.decode(data) do
        {size, digest, value} ->
          # verify header (size, digest)
          ^size   = entry.size - RecordHeader.size
          ^digest = entry.digest

          {digest, value}
        {:error, :malformed} ->
          throw(:bug)
      end
    end)

    # report result
    if reader.batch do
      GenServer.cast(reader.reply_to, {:read_result, reader.segment_id, values})
    else
      GenServer.reply(reader.reply_to, values |> List.first)
    end
    {:stop, :normal, reader}
  end
end
