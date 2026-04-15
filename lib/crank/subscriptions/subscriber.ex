defmodule Crank.Subscriptions.Subscriber do
  use GenServer, restart: :temporary

  alias Crank.Output
  alias Crank.Utils
  @run_supervisor Crank.Subscriptions.RunSupervisor

  def start_link(args) when is_map(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def deliver(pid, signal) when is_pid(pid) do
    send(pid, {:subscription, :signal, signal})
    :ok
  end

  def stop(pid) when is_pid(pid) do
    GenServer.call(pid, :stop, :infinity)
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(args) do
    ref = Map.fetch!(args, :ref)
    name = Map.fetch!(args, :name)
    {:ok, _} = Registry.register(Crank.Subscriptions.SubscriberRegistry, ref, name)

    event_data = %{label: name, now_ms: Utils.now_ms()}
    Output.Server.emit({:ui_pipeline_registered, ref, event_data})

    state = %{
      active_run: nil,
      callback: Map.fetch!(args, :callback),
      ctx: Map.fetch!(args, :base_context),
      name: name,
      on_failure: Map.fetch!(args, :on_failure),
      owner: Map.fetch!(args, :owner),
      pending_signal: nil,
      ref: ref,
      rerun: Map.fetch!(args, :rerun),
      next_run_n: 1,
      stopping?: false
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:stop, _from, %{active_run: nil} = state) do
    notify_owner_terminal_result(state, {:ok, state.ctx})
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    active_run = request_run_stop(state.active_run)
    next_state = %{state | active_run: active_run, pending_signal: nil, stopping?: true}
    {:reply, :ok, next_state}
  end

  @impl true
  def handle_info({:subscription, :signal, _signal}, %{stopping?: true} = state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:subscription, :signal, signal}, %{active_run: nil} = state) do
    emit_signal_event(state, signal, false)
    {:noreply, start_run(state, signal)}
  end

  @impl true
  def handle_info({:subscription, :signal, signal}, state) do
    emit_signal_event(state, signal, true)

    next_state =
      case state.rerun do
        :wait ->
          %{state | pending_signal: signal}

        {:kill, :graceful} ->
          emit_restarting_event(state, signal, :graceful)
          active_run = request_run_stop(state.active_run)
          %{state | active_run: active_run, pending_signal: signal}
      end

    {:noreply, next_state}
  end

  @impl true
  def handle_info(
        {:subscription, :pipeline_id, run_pid, pipeline_id},
        %{active_run: %{pid: run_pid} = active_run} = state
      )
      when is_reference(pipeline_id) do
    if active_run.cancel_requested do
      :ok = Crank.stop(pipeline_id, :graceful)
    end

    run_n = state.next_run_n - 1

    event_data = %{
      now_ms: Utils.now_ms(),
      pipeline_id: pipeline_id,
      run_n: run_n
    }

    Output.Server.emit({:ui_pipeline_run_started, state.ref, event_data})

    next_state = %{state | active_run: %{active_run | pipeline_id: pipeline_id}}
    {:noreply, next_state}
  end

  @impl true
  def handle_info(
        {:subscription, :run_result, run_pid, result},
        %{active_run: %{monitor_ref: monitor_ref, pid: run_pid}} = state
      ) do
    Process.demonitor(monitor_ref, [:flush])

    state
    |> Map.put(:active_run, nil)
    |> finish_run(result)
  end

  @impl true
  def handle_info({:subscription, :pipeline_id, _run_pid, _pipeline_id}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:subscription, :run_result, _run_pid, _result}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:DOWN, monitor_ref, :process, _pid, reason},
        %{active_run: %{monitor_ref: monitor_ref}} = state
      ) do
    state
    |> Map.put(:active_run, nil)
    |> finish_run({:error, {:run_crashed, reason}})
  end

  defp finish_run(%{stopping?: true} = state, _result) do
    notify_owner_terminal_result(state, {:ok, state.ctx})
    {:stop, :normal, state}
  end

  defp finish_run(state, :ok) do
    {:noreply, maybe_start_pending_signal(state)}
  end

  defp finish_run(state, {:ok, new_ctx}) when is_map(new_ctx) do
    next_state = %{state | ctx: new_ctx}
    {:noreply, maybe_start_pending_signal(next_state)}
  end

  defp finish_run(state, {:stop, new_ctx}) when is_map(new_ctx) do
    next_state = %{state | ctx: new_ctx}
    notify_owner_terminal_result(next_state, {:ok, new_ctx})
    {:stop, :normal, next_state}
  end

  defp finish_run(state, {:error, reason}) do
    result = {:error, reason, state.ctx}

    case state.on_failure do
      :continue ->
        {:noreply, maybe_start_pending_signal(state)}

      :drop ->
        notify_owner_terminal_result(state, result)
        {:stop, {:subscription_failed, state.name, reason}, state}
    end
  end

  defp start_run(state, signal) do
    run_n = state.next_run_n
    run_meta = build_run_meta(state, run_n)
    subscriber = self()
    callback = state.callback
    callback_signal = maybe_put_run_n(signal, run_n)
    ctx = state.ctx

    {:ok, pid} =
      DynamicSupervisor.start_child(
        @run_supervisor,
        {Task, fn -> run_callback_task(subscriber, callback, ctx, callback_signal, run_meta) end}
      )

    monitor_ref = Process.monitor(pid)
    active_run = %{cancel_requested: false, monitor_ref: monitor_ref, pid: pid, pipeline_id: nil}
    %{state | active_run: active_run, next_run_n: run_n + 1, pending_signal: nil}
  end

  defp maybe_start_pending_signal(%{pending_signal: nil} = state), do: state

  defp maybe_start_pending_signal(state) do
    start_run(state, state.pending_signal)
  end

  defp request_run_stop(nil), do: nil

  defp request_run_stop(%{cancel_requested: true} = active_run), do: active_run

  defp request_run_stop(active_run) do
    case active_run.pipeline_id do
      pipeline_id when is_reference(pipeline_id) ->
        :ok = Crank.stop(pipeline_id, :graceful)

      nil ->
        Process.exit(active_run.pid, :shutdown)
    end

    %{active_run | cancel_requested: true}
  end

  defp build_run_meta(state, run_n) do
    crank_meta = %{
      logical_id: state.ref,
      run_n: run_n,
      subscription_name: state.name,
      subscription_ref: state.ref
    }

    %{crank: crank_meta}
  end

  defp maybe_put_run_n({:watch, data}, run_n) when is_map(data) do
    next_data = Map.put(data, :run_n, run_n)
    {:watch, next_data}
  end

  defp maybe_put_run_n(signal, _run_n), do: signal

  defp run_callback_task(subscriber, callback, ctx, signal, run_meta) do
    {:ok, _} =
      Registry.register(
        Crank.Subscriptions.RunRegistry,
        self(),
        {subscriber, run_meta}
      )

    result = normalize_callback_result(callback.(ctx, signal))
    Registry.unregister(Crank.Subscriptions.RunRegistry, self())
    notify_run_result(subscriber, result)
  end

  defp normalize_callback_result(:ok), do: :ok
  defp normalize_callback_result({:ok, new_ctx}) when is_map(new_ctx), do: {:ok, new_ctx}
  defp normalize_callback_result({:stop, new_ctx}) when is_map(new_ctx), do: {:stop, new_ctx}
  defp normalize_callback_result(other), do: {:error, {:invalid_return, other}}

  defp notify_run_result(subscriber, result) do
    send(subscriber, {:subscription, :run_result, self(), result})
  end

  defp emit_signal_event(state, signal, pending) do
    event_data = %{
      now_ms: Utils.now_ms(),
      pending: pending,
      signal: signal
    }

    Output.Server.emit({:ui_pipeline_signal, state.ref, event_data})
  end

  defp emit_restarting_event(state, signal, mode) do
    event_data = %{
      mode: mode,
      now_ms: Utils.now_ms(),
      signal: signal
    }

    Output.Server.emit({:ui_pipeline_restarting, state.ref, event_data})
  end

  defp notify_owner_terminal_result(state, result) do
    send(state.owner, {:crank_pipeline_result, state.ref, result})
  end
end
