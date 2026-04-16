defmodule Drum.TmpDir do
  alias Drum.Utils

  @dir_hash_length 8
  @metadata_file ".drum_meta.json"

  defp cache_dir do
    case Application.get_env(:drum, :cache_dir) do
      nil ->
        home = System.user_home!()
        cache = System.get_env("XDG_CACHE_HOME") || Path.join(home, ".cache")
        Path.join(cache, "drum")

      dir ->
        dir
    end
  end

  def tmp_dirs_root, do: Path.join(cache_dir(), "tmp-dirs")
  defp dir_path(key_hash), do: Path.join(tmp_dirs_root(), short_key_hash(key_hash))
  def metadata_path(dir), do: Path.join(dir, @metadata_file)
  def path_for_key(key), do: key |> key_hash() |> dir_path()

  def ensure_dirs(path, ensure_dirs) when is_list(ensure_dirs) do
    Enum.each(ensure_dirs, fn ensure_dir ->
      dir_path = ensured_dir_path(path, ensure_dir)
      File.mkdir_p!(dir_path)
    end)

    :ok
  end

  def ensure_dirs(_path, ensure_dirs) do
    raise ArgumentError,
          "expected :ensure_dirs to be a list of relative paths, got: #{inspect(ensure_dirs)}"
  end

  def sweep do
    with {:ok, entries} <- File.ls(tmp_dirs_root()) do
      entries
      |> Enum.map(&Path.join(tmp_dirs_root(), &1))
      |> Enum.filter(&File.dir?/1)
      |> Enum.each(&remove_if_stale/1)
    end

    :ok
  end

  def create_transient do
    key = {System.pid(), :erlang.unique_integer([:positive])}
    create_persistent(key, {0, :ms})
  end

  def create_persistent(key, ttl) do
    key_hash = key_hash(key)
    now = Utils.now_ms_wall()
    path = path_for_key(key)
    File.mkdir_p!(path)

    old_metadata =
      case read_metadata(path) do
        {:ok, metadata} -> metadata
        _ -> %{}
      end

    updated_metadata = %{
      "pid" => os_pid(),
      "key_hash" => key_hash,
      "created_at" => now,
      "expires_at" => expires_at(ttl, now)
    }

    refreshed_metadata = refresh_metadata(old_metadata, updated_metadata)
    :ok = write_json(metadata_path(path), refreshed_metadata)
    {:ok, path}
  end

  def read_metadata(path), do: read_json(metadata_path(path))

  def update_ttl(path, new_ttl) do
    now = Utils.now_ms_wall()

    with {:ok, metadata} <- read_metadata(path) do
      updated_metadata = Map.put(metadata, "expires_at", expires_at(new_ttl, now))
      write_json(metadata_path(path), updated_metadata)
    end
  end

  def key_hash(key) do
    binary = :erlang.term_to_binary(key)
    :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)
  end

  defp short_key_hash(key_hash) do
    String.slice(key_hash, 0, @dir_hash_length)
  end

  def duration_ms({n, unit}), do: n * unit_ms(unit)

  defp expires_at(:infinity, _now), do: nil
  defp expires_at(ttl, now), do: now + duration_ms(ttl)

  defp remove_if_stale(dir) do
    meta_path = metadata_path(dir)

    with {:ok, metadata} <- read_json(meta_path),
         {:stale, false} <- {:stale, stale?(metadata)} do
      :ok
    else
      _ -> File.rm_rf!(dir)
    end
  end

  defp refresh_metadata(existing_metadata, metadata) do
    existing_metadata
    |> Map.put("pid", metadata["pid"])
    |> Map.put_new("key_hash", metadata["key_hash"])
    |> Map.put_new("created_at", metadata["created_at"])
    |> Map.put_new("expires_at", metadata["expires_at"])
  end

  defp stale?(%{"pid" => pid} = metadata) when is_integer(pid) do
    not pid_alive?(pid) and expired?(metadata)
  end

  defp expired?(%{"expires_at" => nil}), do: false

  defp expired?(%{"expires_at" => expires_at}) do
    Utils.now_ms_wall() >= expires_at
  end

  defp write_json(path, data), do: File.write(path, JSON.encode!(data))

  defp read_json(path) do
    with {:ok, raw} <- File.read(path),
         {:ok, contents} <- JSON.decode(raw) do
      {:ok, contents}
    else
      _ -> :error
    end
  end

  defp os_pid, do: System.pid() |> String.to_integer()

  defp pid_alive?(pid) when is_integer(pid) do
    case System.cmd("kill", ["-0", to_string(pid)], stderr_to_stdout: true) do
      {_, 0} -> true
      _ -> false
    end
  end

  defp unit_ms(u) when u in [:ms, :millisecond, :milliseconds], do: 1
  defp unit_ms(u) when u in [:second, :seconds], do: 1_000
  defp unit_ms(u) when u in [:minute, :minutes], do: 60 * unit_ms(:second)
  defp unit_ms(u) when u in [:hour, :hours], do: 60 * unit_ms(:minute)
  defp unit_ms(u) when u in [:day, :days], do: 24 * unit_ms(:hour)
  defp unit_ms(u) when u in [:week, :weeks], do: 7 * unit_ms(:day)

  defp ensured_dir_path(root, ensure_dir) when is_binary(ensure_dir) do
    expanded_root = Path.expand(root)
    expanded_dir = Path.expand(ensure_dir, expanded_root)

    if String.starts_with?(expanded_dir <> "/", expanded_root <> "/") do
      expanded_dir
    else
      raise ArgumentError,
            "expected ensured dir to stay within #{inspect(root)}, got: #{inspect(ensure_dir)}"
    end
  end

  defp ensured_dir_path(_root, ensure_dir) do
    raise ArgumentError,
          "expected each ensured dir to be a binary path, got: #{inspect(ensure_dir)}"
  end
end
