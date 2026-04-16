defmodule Drum.Cache do
  alias Drum.{TmpDir, Utils}

  def hash_files(globs, opts \\ []) when is_list(globs) and is_list(opts) do
    base = opts |> Keyword.get(:base, File.cwd!()) |> Path.expand()

    path_with_hash = fn {relative_path, path} ->
      {relative_path, hash_file(path)}
    end

    globs
    |> Enum.flat_map(&expand_glob!(&1, base))
    |> Enum.uniq_by(&elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(path_with_hash)
    |> :erlang.term_to_binary()
    |> sha256_hex()
  end

  def with_cache(_run_opts, name, key, opts) when is_list(opts) do
    scope = opts |> Keyword.fetch!(:scope) |> Path.expand()
    ttl = Keyword.fetch!(opts, :ttl)
    miss = Keyword.fetch!(opts, :miss)
    restore = Keyword.fetch!(opts, :restore)

    entry_key = {:cache, scope, name, key}
    entry_dir = TmpDir.path_for_key(entry_key)

    case live_metadata(entry_dir) do
      {:ok, _metadata} ->
        restore.(entry_dir)
        :ok = TmpDir.update_ttl(entry_dir, ttl)
        {:restore, entry_dir}

      :error ->
        File.rm_rf!(entry_dir)
        {:ok, ^entry_dir} = TmpDir.create_persistent(entry_key, {0, :ms})
        miss.(entry_dir)
        :ok = TmpDir.update_ttl(entry_dir, ttl)
        {:miss, entry_dir}
    end
  end

  defp expand_glob!(glob, base) when is_binary(glob) do
    message = "glob #{inspect(glob)} matched no regular files"

    case Path.type(glob) do
      :absolute -> glob
      _ -> Path.join(base, glob)
    end
    |> Path.wildcard()
    |> Enum.map(&Path.expand/1)
    |> Enum.filter(&File.regular?/1)
    |> tap(fn
      [] -> raise(ArgumentError, message)
      _ -> :ok
    end)
    |> Enum.map(&{normalize_relative_path(&1, base), &1})
  end

  defp live_metadata(entry_dir) do
    with {:ok, metadata} <- TmpDir.read_metadata(entry_dir),
         {:live, true} <- {:live, live?(metadata)} do
      {:ok, metadata}
    else
      _ -> :error
    end
  end

  defp live?(%{"expires_at" => nil}), do: true

  defp live?(%{"expires_at" => expires_at}) do
    expires_at > Utils.now_ms_wall()
  end

  defp live?(_metadata), do: false

  defp normalize_relative_path(path, base) do
    path
    |> Path.relative_to(base)
    |> String.replace("\\", "/")
  end

  defp hash_file(path) do
    path
    |> File.stream!([], 65_536)
    |> Enum.reduce(:crypto.hash_init(:sha256), fn chunk, hash ->
      :crypto.hash_update(hash, chunk)
    end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp sha256_hex(binary) do
    :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)
  end
end
