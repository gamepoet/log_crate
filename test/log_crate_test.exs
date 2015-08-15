defmodule LogCrateTest do
  use ExUnit.Case, async: true

  test "it creates an empty crate" do
    c = mk_crate
    assert LogCrate.empty?(c)
  end


  # create an empty crate
  defp mk_crate do
    dir = "#{System.tmp_dir!}/logcrate-test-#{UUID.uuid4(:hex)}"
    File.mkdir_p!(dir)
    System.at_exit(fn(_) -> File.rm_rf!(dir) end)
    {:ok, crate} = LogCrate.create(dir)
    crate
  end
end
