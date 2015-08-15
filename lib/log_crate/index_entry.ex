defmodule LogCrate.IndexEntry do
  defstruct pos: nil,
            size: nil

  def new(pos, size) do
    %__MODULE__{
      pos: pos,
      size: size,
    }
  end
end
