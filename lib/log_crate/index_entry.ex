defmodule LogCrate.IndexEntry do
  defstruct segment_id: nil,
            pos: nil,
            size: nil

  def new(segment_id, pos, size) do
    %__MODULE__{
      segment_id: segment_id,
      pos:        pos,
      size:       size,
    }
  end
end
