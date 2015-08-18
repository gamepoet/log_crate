defmodule LogCrateTest do
  use ExUnit.Case, async: true

  test "it creates an empty crate" do
    with_new_crate(fn(c) ->
      assert LogCrate.empty?(c)
    end)
  end

  test "it fails to create an empty crate if the directory exists" do
    dir = mk_tmpdir
    assert {:error, :directory_exists} == mk_crate(dir)
  end

  test "it can append messages and read them back" do
    with_new_crate(fn(c) ->
      assert 0 == LogCrate.append(c, "hello")
      assert 1 == LogCrate.append(c, "world")
      assert "hello" == LogCrate.read(c, 0)
      assert "world" == LogCrate.read(c, 1)
    end)
  end

  test "it opens saved crates" do
    dir = tmpdir
    with_new_crate(dir, fn(c) ->
      0 = LogCrate.append(c, "some")
      1 = LogCrate.append(c, "data")
    end)

    c = LogCrate.open(dir)
    assert "some" == LogCrate.read(c, 0)
    assert "data" == LogCrate.read(c, 1)
    assert :ok == LogCrate.close(c)
  end

  test "it fails to open a crate if the directory doesn't exist" do
    dir = tmpdir
    assert {:error, :directory_missing} == LogCrate.open(dir)
  end

#  test "it can append messages in batches" do
#    c = with_new_crate(fn(c) ->
#      assert [0,1,2,3] == LogCrate.append(c, ["a", "batch", "of", "messages"])
#    end)
#  end


  # create an empty crate
#  defp mk_crate do
#    mk_crate(tmpdir)
#  end
  # creates a crate from the given directory
  defp mk_crate(dir) when is_binary(dir) do
    LogCrate.create(dir)
  end

  defp with_new_crate(func) when is_function(func, 1) do
    with_new_crate(tmpdir, func)
  end

  defp with_new_crate(dir, func) when is_binary(dir) and is_function(func, 1) do
    c = mk_crate(dir)
    if is_pid(c) do
      try do
        func.(c)
      after
        assert :ok == LogCrate.close(c)
      end
    end
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
