defmodule Drum.Subscriptions.Subscriber do
  use GenServer, restart: :temporary

  alias Drum.Output
  alias Drum.Utils
  @run_supervisor Drum.Subscriptions.RunSupervisor

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
    {:ok, _} = Registry.register(Drum.Subscriptions.SubscriberRegistry, ref, name)

    event_data = %{label: name, now_ms: Utils.now_ms()}
    Output.Server.emit({:ui_pipeline_registered, ref, event_data})

    state = %{
      active_run: nil,
      callback: Map.fetch!(args, :callback),
      ctx: Map.fetch!(args, :base_context),
      idle_timer_ref: nil,
      name: name,
      on_failure: Map.fetch!(args, :on_failure),
      owner: Map.fetch!(args, :owner),
      pending_signal: nil,
      quiescence_check_pending?: false,
      ref: ref,
      rerun: Map.fetch!(args, :rerun),
      next_run_n: 1,
      stop_mode: nil
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

    next_state =
      state
      |> cancel_idle_timer()
      |> Map.put(:active_run, active_run)
      |> Map.put(:pending_signal, nil)
      |> Map.put(:stop_mode, :immediate)

    {:reply, :ok, next_state}
  end

  @impl true
  def handle_info({:subscription, :signal, _signal}, %{stop_mode: :immediate} = state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:subscription, :signal, signal}, %{active_run: nil} = state) do
    next_state = cancel_idle_timer(state)
    emit_signal_event(state, signal, false)
    {:noreply, start_run(next_state, signal)}
  end

  @impl true
  def handle_info({:subscription, :signal, signal}, state) do
    next_state = cancel_idle_timer(state)
    emit_signal_event(state, signal, true)

    next_state =
      case next_state.rerun do
        :wait ->
          %{next_state | pending_signal: signal}

        {:kill, :graceful} ->
          emit_restarting_event(next_state, signal, :graceful)
          active_run = request_run_stop(next_state.active_run)
          %{next_state | active_run: active_run, pending_signal: signal}
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
      :ok = Drum.stop(pipeline_id, :graceful)
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
  def handle_info(:subscription_quiescence_check, state) do
    next_state = %{state | quiescence_check_pending?: false}

    case next_state.stop_mode do
      :drain ->
        maybe_finish_draining(next_state)

      {:idle, idle_ms} ->
        maybe_arm_idle_timer(next_state, idle_ms)

      _ ->
        {:noreply, next_state}
    end
  end

  @impl true
  def handle_info(
        {:subscription, :idle_timeout, idle_timer_ref},
        %{idle_timer_ref: idle_timer_ref, stop_mode: {:idle, _idle_ms}} = state
      ) do
    next_state = %{state | idle_timer_ref: nil}

    case stop_readiness(next_state) do
      :quiescent ->
        stop_normally(next_state)

      :queued ->
        {:noreply, schedule_quiescence_check(next_state)}

      :busy ->
        {:noreply, next_state}
    end
  end

  @impl true
  def handle_info({:subscription, :idle_timeout, _idle_timer_ref}, state) do
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

  defp finish_run(%{stop_mode: :immediate} = state, _result) do
    stop_normally(state)
  end

  defp finish_run(state, :ok) do
    {:noreply, continue_after_run(state)}
  end

  defp finish_run(state, {:ok, new_ctx}) when is_map(new_ctx) do
    next_state = %{state | ctx: new_ctx}
    {:noreply, continue_after_run(next_state)}
  end

  defp finish_run(state, {:stop, new_ctx}) when is_map(new_ctx) do
    next_state = %{state | ctx: new_ctx}
    stop_normally(next_state)
  end

  defp finish_run(state, {:stop, :drain, new_ctx}) when is_map(new_ctx) do
    next_state = %{state | ctx: new_ctx, stop_mode: :drain}
    {:noreply, continue_after_run(next_state)}
  end

  defp finish_run(state, {:stop, {:idle, idle_ms}, new_ctx})
       when is_integer(idle_ms) and idle_ms >= 0 and is_map(new_ctx) do
    next_state = %{state | ctx: new_ctx, stop_mode: {:idle, idle_ms}}
    {:noreply, continue_after_run(next_state)}
  end

  defp finish_run(state, {:error, reason}) do
    result = {:error, reason, state.ctx}

    case state.on_failure do
      :continue ->
        {:noreply, continue_after_run(state)}

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

  defp continue_after_run(state) do
    state
    |> maybe_start_pending_signal()
    |> maybe_schedule_terminal_check()
  end

  defp maybe_schedule_terminal_check(%{stop_mode: stop_mode} = state)
       when stop_mode in [:drain] do
    schedule_quiescence_check(state)
  end

  defp maybe_schedule_terminal_check(%{stop_mode: {:idle, _idle_ms}} = state) do
    schedule_quiescence_check(state)
  end

  defp maybe_schedule_terminal_check(state), do: state

  defp schedule_quiescence_check(%{quiescence_check_pending?: true} = state), do: state

  defp schedule_quiescence_check(state) do
    send(self(), :subscription_quiescence_check)
    %{state | quiescence_check_pending?: true}
  end

  defp request_run_stop(nil), do: nil

  defp request_run_stop(%{cancel_requested: true} = active_run), do: active_run

  defp request_run_stop(active_run) do
    case active_run.pipeline_id do
      pipeline_id when is_reference(pipeline_id) ->
        :ok = Drum.stop(pipeline_id, :graceful)

      nil ->
        Process.exit(active_run.pid, :shutdown)
    end

    %{active_run | cancel_requested: true}
  end

  defp build_run_meta(state, run_n) do
    drum_meta = %{
      logical_id: state.ref,
      run_n: run_n,
      subscription_name: state.name,
      subscription_ref: state.ref
    }

    %{drum: drum_meta}
  end

  defp maybe_put_run_n({:watch, data}, run_n) when is_map(data) do
    next_data = Map.put(data, :run_n, run_n)
    {:watch, next_data}
  end

  defp maybe_put_run_n(signal, _run_n), do: signal

  defp run_callback_task(subscriber, callback, ctx, signal, run_meta) do
    {:ok, _} =
      Registry.register(
        Drum.Subscriptions.RunRegistry,
        self(),
        {subscriber, run_meta}
      )

    result = normalize_callback_result(callback.(ctx, signal))
    Registry.unregister(Drum.Subscriptions.RunRegistry, self())
    notify_run_result(subscriber, result)
  end

  defp normalize_callback_result(:ok), do: :ok
  defp normalize_callback_result({:ok, new_ctx}) when is_map(new_ctx), do: {:ok, new_ctx}
  defp normalize_callback_result({:stop, new_ctx}) when is_map(new_ctx), do: {:stop, new_ctx}

  defp normalize_callback_result({:stop, :drain, new_ctx}) when is_map(new_ctx) do
    {:stop, :drain, new_ctx}
  end

  defp normalize_callback_result({:stop, {:idle, idle_ms}, new_ctx})
       when is_integer(idle_ms) and idle_ms >= 0 and is_map(new_ctx) do
    {:stop, {:idle, idle_ms}, new_ctx}
  end

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
    send(state.owner, {:drum_pipeline_result, state.ref, result})
  end

  defp maybe_finish_draining(state) do
    case stop_readiness(state) do
      :quiescent ->
        stop_normally(state)

      :queued ->
        {:noreply, schedule_quiescence_check(state)}

      :busy ->
        {:noreply, state}
    end
  end

  defp maybe_arm_idle_timer(state, idle_ms) do
    case stop_readiness(state) do
      :quiescent ->
        {:noreply, ensure_idle_timer(state, idle_ms)}

      :queued ->
        {:noreply, schedule_quiescence_check(state)}

      :busy ->
        {:noreply, state}
    end
  end

  defp ensure_idle_timer(%{idle_timer_ref: nil} = state, idle_ms) do
    idle_timer_ref = make_ref()
    Process.send_after(self(), {:subscription, :idle_timeout, idle_timer_ref}, idle_ms)
    %{state | idle_timer_ref: idle_timer_ref}
  end

  defp ensure_idle_timer(state, _idle_ms), do: state

  defp cancel_idle_timer(%{idle_timer_ref: nil} = state), do: state

  defp cancel_idle_timer(state) do
    Process.cancel_timer(state.idle_timer_ref)
    %{state | idle_timer_ref: nil}
  end

  defp stop_readiness(%{active_run: active_run, pending_signal: pending_signal}) do
    cond do
      active_run != nil or pending_signal != nil ->
        :busy

      message_queue_len() > 0 ->
        :queued

      true ->
        :quiescent
    end
  end

  defp message_queue_len do
    {:message_queue_len, queue_len} = Process.info(self(), :message_queue_len)
    queue_len
  end

  defp stop_normally(state) do
    notify_owner_terminal_result(state, {:ok, state.ctx})
    {:stop, :normal, state}
  end
end
