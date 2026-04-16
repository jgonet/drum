defmodule Drum.Subscriptions.Dispatcher do
  alias Drum.Output
  alias Drum.Subscriptions.{PathSpec, Subscriber, SubscriptionOpts, Watcher}
  alias Drum.Utils

  @subscriber_registry Drum.Subscriptions.SubscriberRegistry
  @watcher_registry Drum.Subscriptions.WatcherRegistry
  @subscriber_supervisor Drum.Subscriptions.SubscriberSupervisor
  @watch_supervisor Drum.Subscriptions.WatchSupervisor

  def subscribe(name, callback, opts \\ []) when is_function(callback, 2) do
    normalized_opts = SubscriptionOpts.normalize!(opts)
    ref = make_ref()

    child_args =
      Map.merge(normalized_opts, %{callback: callback, name: name, owner: self(), ref: ref})

    {:ok, _pid} = DynamicSupervisor.start_child(@subscriber_supervisor, {Subscriber, child_args})
    ref
  end

  def unsubscribe(ref) when is_reference(ref) do
    case Registry.lookup(@subscriber_registry, ref) do
      [{pid, _}] -> Subscriber.stop(pid)
      [] -> :ok
    end
  end

  def watch(paths) do
    raw_patterns = normalize_watch_patterns(paths)
    path_specs = PathSpec.normalize_paths!(raw_patterns)
    ref = make_ref()
    child_args = %{path_specs: path_specs, raw_patterns: raw_patterns, ref: ref}

    {:ok, _pid} = DynamicSupervisor.start_child(@watch_supervisor, {Watcher, child_args})
    ref
  end

  def unwatch(ref) when is_reference(ref) do
    case Registry.lookup(@watcher_registry, ref) do
      [{pid, _}] ->
        event_data = %{now_ms: Utils.now_ms()}
        Output.Server.emit({:watcher_removed, ref, event_data})
        stop_watcher(pid)

      [] ->
        :ok
    end
  end

  def notify(signal) do
    validated_signal = validate_signal!(signal)

    @subscriber_registry
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(&Subscriber.deliver(&1, validated_signal))

    :ok
  end

  defp stop_watcher(pid) when is_pid(pid) do
    case DynamicSupervisor.terminate_child(@watch_supervisor, pid) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  catch
    :exit, _reason -> :ok
  end

  defp validate_signal!({type, data} = signal) when is_atom(type) and is_map(data), do: signal

  defp validate_signal!(signal) do
    raise ArgumentError, "expected signal to be {atom, map}, got: #{inspect(signal)}"
  end

  defp normalize_watch_patterns(path) when is_binary(path), do: [path]
  defp normalize_watch_patterns(paths) when is_list(paths), do: paths
end
