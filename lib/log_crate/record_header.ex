defmodule LogCrate.RecordHeader do
  @doc """
  Gets the size of the record header in bytes.
  """
  def size do
    24
  end

  @doc """
  Extracts the header contents from a binary.
  """
  def decode(<<size::integer-size(32), digest::binary-size(20), rest::binary>>) do
    {size, digest, rest}
  end
  def decode(_) do
    {:error, :malformed}
  end

  @doc """
  Encodes a record header based on the given content.
  """
  def encode(size, digest) when is_integer(size) and is_binary(digest) and byte_size(digest) == 20 do
    <<
      size    ::integer-size(32),
      digest  ::binary-size(20),
    >>
  end
end
