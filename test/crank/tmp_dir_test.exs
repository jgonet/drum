defmodule Crank.TmpDirTest do
  use ExUnit.Case, async: false
  import Crank.Test.PipelineHelpers

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:crank, :cache_dir, tmp_dir)
    on_exit(fn -> Application.delete_env(:crank, :cache_dir) end)
    :ok
  end

  describe "tmp_dir!/1" do
    test "tmp_dir! creates a directory that survives pipeline finish" do
      test_pid = self()

      {:ok, pid} =
        Crank.new()
        |> Crank.step("s1", fn _ctx, _run_opts ->
          dir = Crank.tmp_dir!(:transient)
          send(test_pid, {:dir, dir})
        end)
        |> run_pipeline()

      assert_receive {:dir, dir}
      assert File.dir?(dir)
      assert {:ok, _} = await_pipeline(pid)
      assert File.dir?(dir)
    end

    test "tmp_dir! survives pipeline failure" do
      test_pid = self()

      {:ok, pid} =
        Crank.new()
        |> Crank.step("s1", fn _ctx, _run_opts ->
          dir = Crank.tmp_dir!(:transient)
          send(test_pid, {:dir, dir})
          raise "boom"
        end)
        |> run_pipeline()

      assert_receive {:dir, dir}
      assert {:error, _} = await_pipeline(pid)
      assert File.dir?(dir)
    end

    test "multiple tmp_dir! calls create distinct dirs with different keys" do
      test_pid = self()
      ref = make_ref()
      key1 = {:test_diff1, ref}
      key2 = {:test_diff2, ref}

      {:ok, pid} =
        Crank.new()
        |> Crank.step("s1", fn _ctx, _run_opts ->
          dir1 = Crank.tmp_dir!(:transient)
          dir2 = Crank.tmp_dir!(:transient)
          dir3 = Crank.tmp_dir!({:persistent, key: key1, ttl: :infinity})
          dir4 = Crank.tmp_dir!({:persistent, key: key2, ttl: :infinity})
          send(test_pid, {:dirs, dir1, dir2, dir3, dir4})
        end)
        |> run_pipeline()

      assert_receive {:dirs, dir1, dir2, dir3, dir4}
      assert dir1 != dir2
      assert dir3 != dir4
      assert {:ok, _} = await_pipeline(pid)
    end

    test "pipeline cd: fn with tmp_dir! uses it as working directory" do
      test_pid = self()

      {:ok, pid} =
        Crank.new(%{},
          cd: fn _ctx, _run_opts ->
            dir = Crank.tmp_dir!(:transient)
            send(test_pid, {:dir, dir})
            dir
          end
        )
        |> Crank.step("s1", "pwd")
        |> run_pipeline()

      assert_receive {:dir, dir}
      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, Path.basename(dir)))
      assert File.dir?(dir)
    end

    test "tmp_dir! with key returns same path across pipelines" do
      test_pid = self()
      key = {:test_tmp_dir_reuse, make_ref()}
      write_data = fn dir, data -> File.write!(Path.join(dir, "datafile"), data) end
      read_data = fn dir -> File.read!(Path.join(dir, "datafile")) end

      {:ok, pid1} =
        Crank.new()
        |> Crank.step("writer", fn _ctx, _run_opts ->
          dir = Crank.tmp_dir!({:persistent, key: key, ttl: :infinity})
          write_data.(dir, "hello")
          send(test_pid, {:dir1, dir})
        end)
        |> run_pipeline()

      assert_receive {:dir1, tmp_dir}
      assert {:ok, _} = await_pipeline(pid1)

      {:ok, pid2} =
        Crank.new()
        |> Crank.step("reader", fn _ctx, _run_opts ->
          dir = Crank.tmp_dir!({:persistent, key: key, ttl: :infinity})
          send(test_pid, {:dir2, dir, read_data.(dir)})
        end)
        |> run_pipeline()

      assert_receive {:dir2, ^tmp_dir, "hello"}
      assert {:ok, _} = await_pipeline(pid2)
    end
  end

  describe "sweep/0" do
    test "sweep does not delete a live script root while the creator pid is alive" do
      test_pid = self()

      write_data = fn dir, data ->
        File.write!(Path.join(dir, "datafile"), data)
        Path.join(dir, "datafile")
      end

      {:ok, pid} =
        Crank.new()
        |> Crank.step("holder", fn _ctx, _run_opts ->
          dir = Crank.tmp_dir!(:transient)
          filepath = write_data.(dir, "hello")
          send(test_pid, {:held_dir, self(), dir, filepath})

          receive do
            :release -> :ok
          after
            5_000 -> raise "timed out waiting for release"
          end
        end)
        |> run_pipeline()

      assert_receive {:held_dir, holder_pid, dir, filepath}

      Crank.TmpDir.sweep()

      assert File.dir?(dir)
      assert File.exists?(filepath)

      send(holder_pid, :release)
      assert {:ok, _} = await_pipeline(pid)
      assert File.dir?(dir)
    end

    test "sweep removes transient dirs from dead processes and dirs without metadata" do
      stale_dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-deadbeef")
      stale_file = Path.join(stale_dir, "marker")

      meta = %{
        pid: 999_999_999,
        key_hash: "deadbeef",
        created_at: 0,
        expires_at: 0
      }

      File.mkdir_p!(stale_dir)
      File.write!(stale_file, "hello")
      File.write!(Crank.TmpDir.metadata_path(stale_dir), JSON.encode!(meta))

      orphan_dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-orphan")
      orphan_file = Path.join(orphan_dir, "marker")

      File.mkdir_p!(orphan_dir)
      File.write!(orphan_file, "hello")

      Crank.TmpDir.sweep()

      refute File.exists?(stale_dir)
      refute File.exists?(stale_file)
      refute File.exists?(orphan_dir)
      refute File.exists?(orphan_file)
    end

    test "sweep does not remove an expired persistent dir while the owner pid is alive" do
      dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-live-persistent")
      marker = Path.join(dir, "marker")

      meta = %{
        pid: String.to_integer(System.pid()),
        key_hash: "live-persistent",
        created_at: 0,
        expires_at: 0
      }

      File.mkdir_p!(dir)
      File.write!(marker, "hello")
      File.write!(Crank.TmpDir.metadata_path(dir), JSON.encode!(meta))

      Crank.TmpDir.sweep()

      assert File.exists?(dir)
      assert File.exists?(marker)
    end

    test "sweep removes an expired persistent dir when the owner pid is dead" do
      dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-dead-persistent")
      marker = Path.join(dir, "marker")

      meta = %{
        pid: 999_999_999,
        key_hash: "dead-persistent",
        created_at: 0,
        expires_at: 0
      }

      File.mkdir_p!(dir)
      File.write!(marker, "hello")
      File.write!(Crank.TmpDir.metadata_path(dir), JSON.encode!(meta))

      Crank.TmpDir.sweep()

      refute File.exists?(dir)
      refute File.exists?(marker)
    end
  end
end
