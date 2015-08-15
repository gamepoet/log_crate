defmodule LogCrateTest do
  use ExUnit.Case, async: true

  test "it creates an empty crate" do
    c = mk_crate
    assert LogCrate.empty?(c)
  end

  test "it fails to create an empty crate if the directory exists" do
    dir = mk_tmpdir
    assert {:error, :directory_exists} == mk_crate(dir)
  end

  test "it can append messages and read them back" do
    c = mk_crate
    assert 0 == LogCrate.append(c, "hello")
    assert 1 == LogCrate.append(c, "world")
    assert "hello" == LogCrate.read(c, 0)
    assert "world" == LogCrate.read(c, 1)
  end


  # create an empty crate
  defp mk_crate do
    mk_crate(tmpdir)
  end
  # creates a crate from the given directory
  defp mk_crate(dir) do
    LogCrate.create(dir)
  end

  defp tmpdir do
    dir = "#{System.tmp_dir!}/logcrate-test-#{UUID.uuid4(:hex)}"
    on_exit(fn() -> File.rm_rf!(dir) end)
    dir
  end

  defp mk_tmpdir do
    dir = tmpdir
    File.mkdir_p!(dir)
    dir
  end
end
