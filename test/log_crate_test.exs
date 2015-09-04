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

  test "it can append records and read them back" do
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
    assert 2 == LogCrate.append(c, "more!")
    assert :ok == LogCrate.close(c)
  end

  test "it fails to open a crate if the directory doesn't exist" do
    dir = tmpdir
    assert {:error, :directory_missing} == LogCrate.open(dir)
  end

  test "it can append records in batches" do
    with_new_crate(fn(c) ->
      assert [0,1,2,3] == LogCrate.append(c, ["a", "batch", "of", "records"])
    end)
  end

  test "it rolls a new segment when the max size is exceeded" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 8], fn(c) ->
      assert 0 == LogCrate.append(c, "0123456")
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 1 == LogCrate.append(c, "lots and lots more data to push us over");
      assert 2 == File.ls!("#{dir}/") |> Enum.count

      assert "0123456" == LogCrate.read(c, 0)
      assert "lots and lots more data to push us over" == LogCrate.read(c, 1)
    end)
  end

  test "it properly opens crates with multiple segments" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 64], fn(c) ->
      assert 0 == LogCrate.append(c, "0123456")
      assert 1 == LogCrate.append(c, "789abcd")
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 2 == LogCrate.append(c, "something much larger")
      assert 2 == File.ls!("#{dir}/") |> Enum.count
    end)

    c = LogCrate.open(dir)
    assert "0123456" == LogCrate.read(c, 0)
    assert "789abcd" == LogCrate.read(c, 1)
    assert "something much larger" == LogCrate.read(c, 2)
    assert :ok == LogCrate.close(c)
  end

  test "it reports the stored range" do
    with_new_crate(fn(c) ->
      assert nil == LogCrate.range(c)
      assert 0 == LogCrate.append(c, "0123456")
      assert 0..0 == LogCrate.range(c)
      assert 1 == LogCrate.append(c, "789abcd")
      assert 0..1 == LogCrate.range(c)
    end)
  end

  test "it can read in batches" do
    with_new_crate(fn(c) ->
      assert 0 == LogCrate.append(c, "0123456")
      assert 1 == LogCrate.append(c, "789abcd")
      assert 2 == LogCrate.append(c, "something much larger")

      assert ["0123456", "789abcd", "something much larger"] == LogCrate.read(c, 0, 1024)
      assert ["789abcd", "something much larger"] == LogCrate.read(c, 1, 1024)
    end)
  end

  test "it spans segments for batched reads" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 64], fn(c) ->
      assert 0 == LogCrate.append(c, "0123456")
      assert 1 == LogCrate.append(c, "789abcd")
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 2 == LogCrate.append(c, "something much larger")
      assert 2 == File.ls!("#{dir}/") |> Enum.count
      assert 3 == LogCrate.append(c, "more data")

      assert ["789abcd", "something much larger", "more data"] == LogCrate.read(c, 1, 1024)
    end)
  end

  test "it respects the max bytes limit for batched reads" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 64], fn(c) ->
      assert 0 == LogCrate.append(c, "0123456")
      assert 1 == LogCrate.append(c, "789abcd")
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 2 == LogCrate.append(c, "something much larger")
      assert 2 == File.ls!("#{dir}/") |> Enum.count
      assert 3 == LogCrate.append(c, "more data")

      assert [] == LogCrate.read(c, 0, 3)
      assert ["0123456"] == LogCrate.read(c, 0, 7)
      assert ["0123456"] == LogCrate.read(c, 0, 13)
      assert ["0123456", "789abcd"] == LogCrate.read(c, 0, 14)
      assert ["789abcd"] == LogCrate.read(c, 1, 10)
      assert ["789abcd", "something much larger"] == LogCrate.read(c, 1, 30)
    end)
  end

  test "it fails a batched read if the starting offset is unknown" do
    with_new_crate(fn(c) ->
      assert :not_found == LogCrate.read(c, 0, 1024)
      assert 0 == LogCrate.append(c, "0123456")
      assert :not_found == LogCrate.read(c, 1, 1024)
    end)
  end

  # create an empty crate
#  defp mk_crate do
#    mk_crate(tmpdir)
#  end
  # creates a crate from the given directory
  defp mk_crate(dir, opts \\ []) when is_binary(dir) do
    LogCrate.create(dir, opts)
  end

  defp with_new_crate(func) when is_function(func, 1) do
    with_new_crate(tmpdir, func)
  end

  defp with_new_crate(dir, opts \\ [], func) when is_binary(dir) and is_function(func, 1) do
    c = mk_crate(dir, opts)
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
