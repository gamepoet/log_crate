defmodule LogCrate.IndexEntry do
  defstruct segment_id: nil,
            pos:        nil,
            size:       nil,
            digest:     nil

  @type t :: %__MODULE__{
    segment_id: integer,
    pos:        integer,
    size:       integer,
    digest:     binary,
  }

  @spec new(integer, integer, integer, binary) :: t
  def new(segment_id, pos, size, digest) do
    %__MODULE__{
      segment_id: segment_id,
      pos:        pos,
      size:       size,
      digest:     digest,
    }
  end
end
