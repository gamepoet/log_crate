defmodule LogCrate.IndexEntry do
  defstruct segment_id: nil,
            pos: nil,
            size: nil

  @type t :: %__MODULE__{
    segment_id: integer,
    pos:        integer,
    size:       integer,
  }

  @spec new(integer, integer, integer) :: t
  def new(segment_id, pos, size) do
    %__MODULE__{
      segment_id: segment_id,
      pos:        pos,
      size:       size,
    }
  end
end
