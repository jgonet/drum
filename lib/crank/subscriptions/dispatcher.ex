defmodule Crank.Subscriptions.Dispatcher do
  alias Crank.Subscriptions.{PathSpec, Subscriber, SubscriptionOpts, Watcher}

  @subscriber_registry Crank.Subscriptions.SubscriberRegistry
  @watcher_registry Crank.Subscriptions.WatcherRegistry
  @subscriber_supervisor Crank.Subscriptions.SubscriberSupervisor
  @watch_supervisor Crank.Subscriptions.WatchSupervisor

  def subscribe(name, callback, opts \\ []) when is_function(callback, 2) do
    normalized_opts = SubscriptionOpts.normalize!(opts)
    ref = make_ref()

    child_args =
      Map.merge(normalized_opts, %{callback: callback, name: name, ref: ref})

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
    path_specs = PathSpec.normalize_paths!(paths)
    ref = make_ref()
    child_args = %{path_specs: path_specs, ref: ref}

    {:ok, _pid} = DynamicSupervisor.start_child(@watch_supervisor, {Watcher, child_args})
    ref
  end

  def unwatch(ref) when is_reference(ref) do
    case Registry.lookup(@watcher_registry, ref) do
      [{pid, _}] -> stop_watcher(pid)
      [] -> :ok
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
end
