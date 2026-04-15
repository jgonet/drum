defmodule Crank.CacheTest do
  use ExUnit.Case, async: true

  import Crank.Test.PipelineHelpers

  @ttl {1, :day}
  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    scope = Path.join(tmp_dir, "scope")
    restore_root = Path.join(tmp_dir, "restore")

    File.mkdir_p!(scope)
    File.mkdir_p!(restore_root)

    {:ok, scope: scope, restore_root: restore_root}
  end

  describe "hash_files/2: stable hashing" do
    test "same inputs produce the same key", %{scope: scope} do
      write_file(scope, "lib/example.ex", "IO.puts(:ok)\n")
      write_file(scope, "mix.exs", "defmodule Example.MixProject do end\n")

      first_key = Crank.Cache.hash_files(["lib/**/*.ex", "mix.exs"], base: scope)
      second_key = Crank.Cache.hash_files(["lib/**/*.ex", "mix.exs"], base: scope)

      assert first_key == second_key
    end

    test "different file contents produce different keys", %{scope: scope} do
      write_file(scope, "lib/example.ex", "IO.puts(:one)\n")

      first_key = Crank.Cache.hash_files(["lib/**/*.ex"], base: scope)

      write_file(scope, "lib/example.ex", "IO.puts(:two)\n")

      second_key = Crank.Cache.hash_files(["lib/**/*.ex"], base: scope)

      refute first_key == second_key
    end

    test "duplicate matches across globs are deduped", %{scope: scope} do
      write_file(scope, "lib/example.ex", "IO.puts(:ok)\n")

      first_key = Crank.Cache.hash_files(["lib/**/*.ex"], base: scope)

      second_key =
        Crank.Cache.hash_files(["lib/**/*.ex", "lib/example.ex"], base: scope)

      assert first_key == second_key
    end

    test "raises when any glob matches no regular files", %{scope: scope} do
      write_file(scope, "lib/example.ex", "IO.puts(:ok)\n")
      File.mkdir_p!(Path.join(scope, "empty"))

      assert_raise ArgumentError, ~r/matched no regular files/, fn ->
        Crank.Cache.hash_files(["lib/**/*.ex", "empty/*"], base: scope)
      end
    end
  end

  describe "with_cache/4: cache lifecycle" do
    test "cache miss publishes only after miss succeeds", %{scope: scope} do
      key = unique_key("publish")
      entry_dir = cache_entry_dir(scope, "compile", key)
      register_tmpdir_cleanup(entry_dir)

      assert {:miss, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn dir ->
                   send(self(), {:miss, dir})
                   assert dir == entry_dir
                   assert {:ok, metadata} = Crank.TmpDir.read_metadata(dir)
                   assert is_integer(metadata["expires_at"])
                   assert metadata["expires_at"] <= System.system_time(:millisecond)
                   File.write!(Path.join(dir, "artifact"), "built")
                 end,
                 restore: fn _dir ->
                   send(self(), :restore_called)
                 end
               )

      assert_received {:miss, ^entry_dir}
      refute_received :restore_called
      assert {:ok, metadata} = Crank.TmpDir.read_metadata(entry_dir)
      assert metadata["expires_at"] > System.system_time(:millisecond)
      assert "built" = File.read!(Path.join(entry_dir, "artifact"))
    end

    test "cache hit calls only restore and bumps ttl", %{scope: scope, restore_root: restore_root} do
      key = unique_key("hit")
      entry_dir = cache_entry_dir(scope, "compile", key)
      register_tmpdir_cleanup(entry_dir)

      assert {:miss, ^entry_dir} =
               run_cache(scope, key,
                 ttl: {1, :second},
                 miss: fn dir ->
                   send(self(), {:miss, dir})
                   File.write!(Path.join(dir, "artifact"), "built")
                 end,
                 restore: fn _dir ->
                   send(self(), :restore_called)
                 end
               )

      assert_received {:miss, ^entry_dir}
      refute_received :restore_called
      assert {:ok, before_restore_metadata} = Crank.TmpDir.read_metadata(entry_dir)
      before_restore_expires_at = before_restore_metadata["expires_at"]

      restored_path = Path.join(restore_root, "artifact")

      assert {:restore, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn _dir ->
                   send(self(), :miss_called_again)
                 end,
                 restore: fn dir ->
                   File.cp!(Path.join(dir, "artifact"), restored_path)
                   send(self(), {:restore, dir})
                 end
               )

      refute_received :miss_called_again
      assert_received {:restore, ^entry_dir}
      assert {:ok, after_restore_metadata} = Crank.TmpDir.read_metadata(entry_dir)
      assert after_restore_metadata["expires_at"] > before_restore_expires_at
      assert "built" = File.read!(restored_path)
    end

    test "different keys map to different entry dirs", %{scope: scope} do
      first_key = unique_key("first")
      second_key = unique_key("second")

      first_dir = cache_entry_dir(scope, "compile", first_key)
      second_dir = cache_entry_dir(scope, "compile", second_key)

      register_tmpdir_cleanup(first_dir)
      register_tmpdir_cleanup(second_dir)

      refute first_dir == second_dir
    end

    test "expired entry skips restore and rebuilds", %{scope: scope} do
      key = unique_key("expired")
      entry_dir = cache_entry_dir(scope, "compile", key)
      register_tmpdir_cleanup(entry_dir)

      assert {:miss, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn dir ->
                   File.write!(Path.join(dir, "artifact"), "stale")
                 end,
                 restore: fn _dir ->
                   send(self(), :restore_called)
                 end
               )

      assert :ok = Crank.TmpDir.update_ttl(entry_dir, {0, :ms})

      assert {:miss, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn dir ->
                   send(self(), {:rebuilt, dir})
                   assert not File.exists?(Path.join(dir, "artifact"))
                   File.write!(Path.join(dir, "artifact"), "fresh")
                 end,
                 restore: fn _dir ->
                   send(self(), :restore_called)
                 end
               )

      refute_received :restore_called
      assert_received {:rebuilt, ^entry_dir}
      assert "fresh" = File.read!(Path.join(entry_dir, "artifact"))
    end

    test "failed miss stays expired and is never treated as a hit", %{scope: scope} do
      key = unique_key("failed-miss")
      entry_dir = cache_entry_dir(scope, "compile", key)
      register_tmpdir_cleanup(entry_dir)

      assert_raise RuntimeError, "boom", fn ->
        run_cache(scope, key,
          miss: fn dir ->
            File.write!(Path.join(dir, "partial"), "leftover")
            raise "boom"
          end,
          restore: fn _dir ->
            send(self(), :restore_called)
          end
        )
      end

      assert {:ok, metadata} = Crank.TmpDir.read_metadata(entry_dir)
      assert metadata["expires_at"] <= System.system_time(:millisecond)

      assert {:miss, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn dir ->
                   send(self(), {:retry_miss, dir})
                   assert not File.exists?(Path.join(dir, "partial"))
                   File.write!(Path.join(dir, "artifact"), "rebuilt")
                 end,
                 restore: fn _dir ->
                   send(self(), :restore_called)
                 end
               )

      refute_received :restore_called
      assert_received {:retry_miss, ^entry_dir}
      assert "rebuilt" = File.read!(Path.join(entry_dir, "artifact"))
    end

    test "restore failure crashes and does not rebuild", %{
      scope: scope,
      restore_root: restore_root
    } do
      key = unique_key("restore-retry")
      entry_dir = cache_entry_dir(scope, "compile", key)
      register_tmpdir_cleanup(entry_dir)

      assert {:miss, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn dir ->
                   File.write!(Path.join(dir, "artifact"), "published")
                 end,
                 restore: fn _dir ->
                   send(self(), :unexpected_restore)
                 end
               )

      refute_received :unexpected_restore

      assert_raise RuntimeError, "corrupt restore", fn ->
        run_cache(scope, key,
          miss: fn _dir ->
            send(self(), :retry_miss)
          end,
          restore: fn _dir ->
            send(self(), :restore_attempt)
            raise "corrupt restore"
          end
        )
      end

      assert_received :restore_attempt
      refute_received :retry_miss

      restored_path = Path.join(restore_root, "artifact")

      assert {:restore, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn _dir ->
                   flunk("expected cache hit")
                 end,
                 restore: fn dir ->
                   File.cp!(Path.join(dir, "artifact"), restored_path)
                 end
               )

      assert "published" = File.read!(restored_path)
    end

    test "later pipeline failure does not invalidate a published cache entry", %{
      scope: scope,
      restore_root: restore_root
    } do
      key = unique_key("pipeline-failure")
      entry_dir = cache_entry_dir(scope, "compile", key)
      register_tmpdir_cleanup(entry_dir)

      {:ok, pipeline_id} =
        Crank.new(%{scope: scope, key: key})
        |> Crank.step("publish", fn ctx, run_opts ->
          Crank.with_cache(run_opts, "compile", ctx.key,
            scope: ctx.scope,
            ttl: @ttl,
            miss: fn dir ->
              File.write!(Path.join(dir, "artifact"), "published")
            end,
            restore: fn _dir ->
              flunk("expected initial miss")
            end
          )
        end)
        |> Crank.step("fail", fn _ctx, _run_opts ->
          raise "later failure"
        end)
        |> run_pipeline()

      assert {:error, _data} = await_pipeline(pipeline_id)

      restored_path = Path.join(restore_root, "artifact")

      assert {:restore, ^entry_dir} =
               run_cache(scope, key,
                 miss: fn _dir ->
                   flunk("expected published cache entry to remain valid")
                 end,
                 restore: fn dir ->
                   File.cp!(Path.join(dir, "artifact"), restored_path)
                 end
               )

      assert "published" = File.read!(restored_path)
    end
  end

  defp write_file(root, relative_path, contents) do
    path = Path.join(root, relative_path)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, contents)
  end

  defp unique_key(name) do
    {name, make_ref()}
  end

  defp cache_entry_dir(scope, name, key) do
    {:cache, Path.expand(scope), name, key}
    |> Crank.TmpDir.path_for_key()
  end

  defp run_cache(scope, key, opts) do
    {ttl, rest} = Keyword.pop(opts, :ttl, @ttl)
    Crank.Cache.with_cache(%{}, "compile", key, [scope: scope, ttl: ttl] ++ rest)
  end
end
