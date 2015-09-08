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
      assert 0 == LogCrate.append(c, mk_record("hello"))
      assert 1 == LogCrate.append(c, mk_record("world"))
      assert mk_record("hello") == LogCrate.read(c, 0)
      assert mk_record("world") == LogCrate.read(c, 1)
    end)
  end

  test "it opens saved crates" do
    dir = tmpdir
    with_new_crate(dir, fn(c) ->
      0 = LogCrate.append(c, mk_record("some"))
      1 = LogCrate.append(c, mk_record("data"))
    end)

    c = LogCrate.open(dir)
    assert mk_record("some") == LogCrate.read(c, 0)
    assert mk_record("data") == LogCrate.read(c, 1)
    assert 2 == LogCrate.append(c, mk_record("more!"))
    assert :ok == LogCrate.close(c)
  end

  test "it fails to open a crate if the directory doesn't exist" do
    dir = tmpdir
    assert {:error, :directory_missing} == LogCrate.open(dir)
  end

  test "it can append records in batches" do
    with_new_crate(fn(c) ->
      assert [0,1,2,3] == LogCrate.append(c, [mk_record("a"), mk_record("batch"), mk_record("of"), mk_record("records")])
    end)
  end

  test "it rolls a new segment when the max size is exceeded" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 8], fn(c) ->
      assert 0 == LogCrate.append(c, mk_record("0123456"))
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 1 == LogCrate.append(c, mk_record("lots and lots more data to push us over"))
      assert 2 == File.ls!("#{dir}/") |> Enum.count

      assert mk_record("0123456") == LogCrate.read(c, 0)
      assert mk_record("lots and lots more data to push us over") == LogCrate.read(c, 1)
    end)
  end

  test "it properly opens crates with multiple segments" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 90], fn(c) ->
      assert 0 == LogCrate.append(c, mk_record("0123456"))
      assert 1 == LogCrate.append(c, mk_record("789abcd"))
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 2 == LogCrate.append(c, mk_record("something much larger"))
      assert 2 == File.ls!("#{dir}/") |> Enum.count
    end)

    c = LogCrate.open(dir)
    assert mk_record("0123456") == LogCrate.read(c, 0)
    assert mk_record("789abcd") == LogCrate.read(c, 1)
    assert mk_record("something much larger") == LogCrate.read(c, 2)
    assert :ok == LogCrate.close(c)
  end

  test "it reports the stored range" do
    with_new_crate(fn(c) ->
      assert nil == LogCrate.range(c)
      assert 0 == LogCrate.append(c, mk_record("0123456"))
      assert 0..0 == LogCrate.range(c)
      assert 1 == LogCrate.append(c, mk_record("789abcd"))
      assert 0..1 == LogCrate.range(c)
    end)
  end

  test "it can read in batches" do
    with_new_crate(fn(c) ->
      assert 0 == LogCrate.append(c, mk_record("0123456"))
      assert 1 == LogCrate.append(c, mk_record("789abcd"))
      assert 2 == LogCrate.append(c, mk_record("something much larger"))

      assert [mk_record("0123456"), mk_record("789abcd"), mk_record("something much larger")] == LogCrate.read(c, 0, 1024)
      assert [mk_record("789abcd"), mk_record("something much larger")] == LogCrate.read(c, 1, 1024)
    end)
  end

  test "it spans segments for batched reads" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 90], fn(c) ->
      assert 0 == LogCrate.append(c, mk_record("0123456"))
      assert 1 == LogCrate.append(c, mk_record("789abcd"))
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 2 == LogCrate.append(c, mk_record("something much larger"))
      assert 2 == File.ls!("#{dir}/") |> Enum.count
      assert 3 == LogCrate.append(c, mk_record("more data"))

      assert [mk_record("789abcd"), mk_record("something much larger"), mk_record("more data")] == LogCrate.read(c, 1, 1024)
    end)
  end

  test "it respects the max bytes limit for batched reads" do
    dir = tmpdir
    with_new_crate(dir, [segment_max_size: 90], fn(c) ->
      assert 0 == LogCrate.append(c, mk_record("0123456"))
      assert 1 == LogCrate.append(c, mk_record("789abcd"))
      assert 1 == File.ls!("#{dir}/") |> Enum.count
      assert 2 == LogCrate.append(c, mk_record("something much larger"))
      assert 2 == File.ls!("#{dir}/") |> Enum.count
      assert 3 == LogCrate.append(c, mk_record("more data"))

      assert [] == LogCrate.read(c, 0, 3)
      assert [mk_record("0123456")] == LogCrate.read(c, 0, 7)
      assert [mk_record("0123456")] == LogCrate.read(c, 0, 13)
      assert [mk_record("0123456"), mk_record("789abcd")] == LogCrate.read(c, 0, 14)
      assert [mk_record("789abcd")] == LogCrate.read(c, 1, 10)
      assert [mk_record("789abcd"), mk_record("something much larger")] == LogCrate.read(c, 1, 30)
    end)
  end

  test "it fails a batched read if the starting offset is unknown" do
    with_new_crate(fn(c) ->
      assert :not_found == LogCrate.read(c, 0, 1024)
      assert 0 == LogCrate.append(c, mk_record("0123456"))
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

  defp sha1(data) when is_binary(data) do
    :crypto.hash(:sha, data)
  end

  defp mk_record(data) when is_binary(data) do
    {sha1(data), data}
  end
end
