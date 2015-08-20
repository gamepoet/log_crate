defmodule LogCrate.Config do
  @opaque t :: %__MODULE__{
    dir: binary,

  }
  defstruct dir:              nil,
            segment_max_size: nil

  @default_opts [
    segment_max_size: 512 * 1024 * 1024,
  ]

  @spec new(binary, Keyword.t) :: t
  def new(dir, opts \\ []) do
    struct(%__MODULE__{dir: dir}, Keyword.merge(@default_opts, opts))
  end
end
