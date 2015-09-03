defmodule LogCrate.BatchReader do
  alias __MODULE__
  alias LogCrate.Reader
  use GenServer

  defstruct reply_to:      nil,
            dir:           nil,
            read_list:     nil,
            request_count: nil,
            results:       nil

  def start_link(reply_to, dir, read_list) do
    GenServer.start_link(__MODULE__, {reply_to, dir, read_list})
  end

  #
  # GenServer callbacks
  #

  def init({reply_to, dir, read_list}) do
    batch_reader = %BatchReader{
      reply_to:  reply_to,
      dir:       dir,
      read_list: read_list,
    }
    {:ok, batch_reader, 0}
  end

  def handle_info(:timeout, batch_reader) do
    # group the reading list by segment
    read_list = Enum.group_by(batch_reader.read_list, fn(segment) -> segment.segment_id end)
    read_list = Enum.reduce(read_list, %{}, fn({segment_id, list}, accum) ->
      Dict.put(accum, segment_id, Enum.reverse(list))
    end)

    # issue read requests to all the segments
    Enum.each(read_list, fn({segment_id, list}) ->
      Reader.start_link(self, batch_reader.dir, segment_id, list, true)
    end)

    batch_reader = %{batch_reader |
      read_list:     nil,
      request_count: Enum.count(read_list),
      results:       %{}
    }

    {:noreply, batch_reader}
  end

  def handle_cast({:read_result, segment_id, data}, batch_reader) do
    results = Dict.put(batch_reader.results, segment_id, data)
    if Enum.count(results) != batch_reader.request_count do
      # there are still outstanding reads, so keep waiting
      batch_reader = %{batch_reader | results: results}
      {:noreply, batch_reader}
    else
      # the final read came in, merge and report results
      segment_ids = Dict.keys(results) |> Enum.sort
      all_data = Enum.map(segment_ids, fn(segment_id) ->
        Dict.get(results, segment_id)
      end) |> Enum.concat

      GenServer.reply(batch_reader.reply_to, all_data)
      {:stop, :normal, batch_reader}
    end
  end
end
