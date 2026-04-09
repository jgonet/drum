defmodule Crank.Step.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Registry, Utils}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(%{step: step, pipeline_id: pipeline_id, ctx: ctx} = args) do
    group_id = Map.get(args, :group_id)
    parent_cd = Map.get(args, :parent_cd)
    run_opts = %{worker_sup: Registry.worker_sup(pipeline_id), pipeline_id: pipeline_id, step_id: step.id, cd: parent_cd}
    effective_cd = Utils.resolve_cd(step.cd, ctx, run_opts) || parent_cd

    state = %{
      id: step.id,
      name: step.name,
      action: step.action,
      pipeline_id: pipeline_id,
      group_id: group_id,
      ctx: ctx,
      cd: effective_cd,
      timeout: step.timeout,
      spawn_pid: nil,
      timer_ref: nil
    }

    event_data = %{id: step.id, name: step.name, pipeline_id: pipeline_id, group_id: group_id, now_ms: Utils.now_ms()}
    Output.Server.emit({:step_started, pipeline_id, event_data})
    {:ok, state, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, state) do
    server = self()

    cmd_opts = %{
      worker_sup: Registry.worker_sup(state.pipeline_id),
      pipeline_id: state.pipeline_id,
      step_id: state.id,
      cd: state.cd
    }

    spawn_pid =
      spawn_link(fn ->
        {result, ctx_op} =
          try do
            raw = state.action.(state.ctx, cmd_opts)

            ctx_op =
              case raw do
                {:ctx_add, map} when is_map(map) -> {:ctx_add, map}
                {:ctx_set, map} when is_map(map) -> {:ctx_set, map}
                _ -> nil
              end

            {:ok, ctx_op}
          rescue
            e -> {{:error, {:action_error, e}}, nil}
          end

        send(server, {:action_done, result, ctx_op})
      end)

    timer_ref = schedule_timeout(state.timeout)
    {:noreply, %{state | spawn_pid: spawn_pid, timer_ref: timer_ref}}
  end

  @impl true
  def handle_info({:action_done, :ok, ctx_op}, state) do
    cancel_timeout(state.timer_ref)

    event_data = %{
      id: state.id,
      name: state.name,
      pipeline_id: state.pipeline_id,
      group_id: state.group_id,
      now_ms: Utils.now_ms()
    }

    Output.Server.emit({:step_finished, state.pipeline_id, event_data})
    notify_done(state, :ok, ctx_op)
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:action_done, {:error, reason}, _ctx_op}, state) do
    cancel_timeout(state.timer_ref)
    emit_step_failed(state, reason)
    notify_done(state, {:error, reason}, nil)
    {:stop, {:shutdown, :action_failed}, state}
  end

  @impl true
  def handle_info(:step_timeout, state) do
    Process.unlink(state.spawn_pid)
    Process.exit(state.spawn_pid, :kill)
    emit_step_failed(state, :timeout)
    notify_done(state, {:error, :timeout}, nil)
    {:stop, {:shutdown, :action_failed}, state}
  end

  defp emit_step_failed(state, reason) do
    event_data = %{
      id: state.id,
      name: state.name,
      pipeline_id: state.pipeline_id,
      group_id: state.group_id,
      reason: reason,
      now_ms: Utils.now_ms()
    }

    Output.Server.emit({:step_failed, state.pipeline_id, event_data})
  end

  defp notify_done(%{group_id: nil} = state, result, ctx_op) do
    pipeline = Registry.pipeline(state.pipeline_id)
    GenServer.cast(pipeline, {:step_done, state.id, result, ctx_op})
  end

  defp notify_done(state, result, ctx_op) do
    group = Registry.group(state.pipeline_id, state.group_id)
    GenServer.cast(group, {:step_done, state.id, result, ctx_op})
  end

  defp schedule_timeout(nil), do: nil
  defp schedule_timeout(:infinity), do: nil
  defp schedule_timeout(ms) when is_integer(ms), do: Process.send_after(self(), :step_timeout, ms)

  defp cancel_timeout(nil), do: :ok
  defp cancel_timeout(ref), do: Process.cancel_timer(ref)
end
