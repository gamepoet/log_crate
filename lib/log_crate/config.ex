defmodule LogCrate.Config do
  @opaque t :: %__MODULE__{
    dir: binary
  }
  defstruct dir: nil
end
