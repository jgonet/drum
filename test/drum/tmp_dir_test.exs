defmodule Drum.TmpDirTest do
  use ExUnit.Case, async: false
  import Drum.Test.PipelineHelpers

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:drum, :cache_dir, tmp_dir)
    on_exit(fn -> Application.delete_env(:drum, :cache_dir) end)
    :ok
  end

  describe "tmp_dir!/1" do
    test "tmp_dir! creates a directory that survives pipeline finish" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.step("s1", fn _ctx, _run_opts ->
          dir = Drum.tmp_dir!(:transient)
          send(test_pid, {:dir, dir})
        end)
        |> run_pipeline()

      assert_receive {:dir, dir}
      assert File.dir?(dir)
      assert Path.basename(dir) =~ ~r/^[0-9a-f]{8}$/
      assert {:ok, _} = await_pipeline(pid)
      assert File.dir?(dir)
    end

    test "tmp_dir! survives pipeline failure" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.step("s1", fn _ctx, _run_opts ->
          dir = Drum.tmp_dir!(:transient)
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
        Drum.new()
        |> Drum.step("s1", fn _ctx, _run_opts ->
          dir1 = Drum.tmp_dir!(:transient)
          dir2 = Drum.tmp_dir!(:transient)
          dir3 = Drum.tmp_dir!({:persistent, key: key1, ttl: :infinity})
          dir4 = Drum.tmp_dir!({:persistent, key: key2, ttl: :infinity})
          send(test_pid, {:dirs, dir1, dir2, dir3, dir4})
        end)
        |> run_pipeline()

      assert_receive {:dirs, dir1, dir2, dir3, dir4}
      assert dir1 != dir2
      assert dir3 != dir4
      assert {:ok, _} = await_pipeline(pid)
    end

    test "tmp_dir! can ensure child directories exist" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.step("s1", fn _ctx, _run_opts ->
          dir = Drum.tmp_dir!({:transient, ensure_dirs: ["src", "lib/vm", "c_src"]})
          src_dir = Path.join(dir, "src")
          vm_dir = Path.join(dir, "lib/vm")
          c_src_dir = Path.join(dir, "c_src")
          send(test_pid, {:dirs, dir, src_dir, vm_dir, c_src_dir})
        end)
        |> run_pipeline()

      assert_receive {:dirs, dir, src_dir, vm_dir, c_src_dir}
      assert File.dir?(dir)
      assert File.dir?(src_dir)
      assert File.dir?(vm_dir)
      assert File.dir?(c_src_dir)
      assert {:ok, _} = await_pipeline(pid)
    end

    test "pipeline cd: fn with tmp_dir! uses it as working directory" do
      test_pid = self()

      {:ok, pid} =
        Drum.new(%{},
          cd: fn _ctx, _run_opts ->
            dir = Drum.tmp_dir!(:transient)
            send(test_pid, {:dir, dir})
            dir
          end
        )
        |> Drum.step("s1", "pwd")
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
        Drum.new()
        |> Drum.step("writer", fn _ctx, _run_opts ->
          dir = Drum.tmp_dir!({:persistent, key: key, ttl: :infinity})
          write_data.(dir, "hello")
          send(test_pid, {:dir1, dir})
        end)
        |> run_pipeline()

      assert_receive {:dir1, tmp_dir}
      assert {:ok, _} = await_pipeline(pid1)

      {:ok, pid2} =
        Drum.new()
        |> Drum.step("reader", fn _ctx, _run_opts ->
          dir = Drum.tmp_dir!({:persistent, key: key, ttl: :infinity})
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
        Drum.new()
        |> Drum.step("holder", fn _ctx, _run_opts ->
          dir = Drum.tmp_dir!(:transient)
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

      Drum.TmpDir.sweep()

      assert File.dir?(dir)
      assert File.exists?(filepath)

      send(holder_pid, :release)
      assert {:ok, _} = await_pipeline(pid)
      assert File.dir?(dir)
    end

    test "sweep removes transient dirs from dead processes and dirs without metadata" do
      stale_dir = Path.join(Drum.TmpDir.tmp_dirs_root(), "deadbeef")
      stale_file = Path.join(stale_dir, "marker")

      meta = %{
        pid: 999_999_999,
        key_hash: "deadbeef",
        created_at: 0,
        expires_at: 0
      }

      File.mkdir_p!(stale_dir)
      File.write!(stale_file, "hello")
      File.write!(Drum.TmpDir.metadata_path(stale_dir), JSON.encode!(meta))

      orphan_dir = Path.join(Drum.TmpDir.tmp_dirs_root(), "orphan")
      orphan_file = Path.join(orphan_dir, "marker")

      File.mkdir_p!(orphan_dir)
      File.write!(orphan_file, "hello")

      Drum.TmpDir.sweep()

      refute File.exists?(stale_dir)
      refute File.exists?(stale_file)
      refute File.exists?(orphan_dir)
      refute File.exists?(orphan_file)
    end

    test "sweep does not remove an expired persistent dir while the owner pid is alive" do
      dir = Path.join(Drum.TmpDir.tmp_dirs_root(), "live-persistent")
      marker = Path.join(dir, "marker")

      meta = %{
        pid: String.to_integer(System.pid()),
        key_hash: "live-persistent",
        created_at: 0,
        expires_at: 0
      }

      File.mkdir_p!(dir)
      File.write!(marker, "hello")
      File.write!(Drum.TmpDir.metadata_path(dir), JSON.encode!(meta))

      Drum.TmpDir.sweep()

      assert File.exists?(dir)
      assert File.exists?(marker)
    end

    test "sweep removes an expired persistent dir when the owner pid is dead" do
      dir = Path.join(Drum.TmpDir.tmp_dirs_root(), "dead-persistent")
      marker = Path.join(dir, "marker")

      meta = %{
        pid: 999_999_999,
        key_hash: "dead-persistent",
        created_at: 0,
        expires_at: 0
      }

      File.mkdir_p!(dir)
      File.write!(marker, "hello")
      File.write!(Drum.TmpDir.metadata_path(dir), JSON.encode!(meta))

      Drum.TmpDir.sweep()

      refute File.exists?(dir)
      refute File.exists?(marker)
    end
  end
end
