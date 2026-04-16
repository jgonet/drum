defmodule Drum.Subscriptions.Watcher do
  use GenServer, restart: :temporary

  alias Drum.Output
  alias Drum.Subscriptions.{Dispatcher, PathSpec}
  alias Drum.Utils

  def start_link(args) when is_map(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    path_specs = Map.fetch!(args, :path_specs)
    raw_patterns = Map.fetch!(args, :raw_patterns)
    ref = Map.fetch!(args, :ref)
    paths = path_specs |> Enum.map(& &1.path) |> Enum.uniq()

    extra_opts = Application.get_env(:drum, :file_system_opts, [])

    case FileSystem.start_link([dirs: paths] ++ extra_opts) do
      {:ok, watcher_pid} ->
        :ok = FileSystem.subscribe(watcher_pid)
        {:ok, _} = Registry.register(Drum.Subscriptions.WatcherRegistry, ref, nil)

        event_data = %{
          now_ms: Utils.now_ms(),
          patterns: raw_patterns,
          roots: paths
        }

        Output.Server.emit({:watcher_registered, ref, event_data})

        state = %{
          path_specs: path_specs,
          ref: ref,
          watcher_pid: watcher_pid
        }

        {:ok, state}

      {:error, reason} ->
        {:stop, reason}

      :ignore ->
        {:stop, :ignore}
    end
  end

  @impl true
  def handle_info({:file_event, watcher_pid, {path, _events}}, state) do
    if watcher_pid == state.watcher_pid do
      changed =
        path
        |> normalize_changed_paths()
        |> filter_changed_paths(state.path_specs)

      if changed != [] do
        watcher_event_data = %{
          changed: changed,
          now_ms: Utils.now_ms()
        }

        Output.Server.emit({:watcher_updated, state.ref, watcher_event_data})

        event_data = %{watch: state.ref, changed: changed}
        Dispatcher.notify({:watch, event_data})
      end
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:file_event, watcher_pid, :stop}, state) do
    if watcher_pid == state.watcher_pid do
      event_data = %{now_ms: Utils.now_ms()}
      Output.Server.emit({:watcher_removed, state.ref, event_data})
      {:stop, :normal, state}
    else
      {:noreply, state}
    end
  end

  defp normalize_changed_paths(path) when is_binary(path), do: [Path.expand(path)]

  defp normalize_changed_paths(paths) when is_list(paths) do
    paths
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&Path.expand/1)
  end

  defp normalize_changed_paths(_other), do: []

  defp filter_changed_paths(changed_paths, path_specs) do
    changed_paths
    |> Enum.filter(fn changed_path ->
      Enum.any?(path_specs, &PathSpec.match?(&1, changed_path))
    end)
    |> Enum.uniq()
  end
end
