defmodule Crank.Group.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Registry, Step, Utils}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args.name)
  end

  @impl true
  def init(%{group: group, pipeline_id: pipeline_id, ctx: ctx} = args) do
    parent_cd = Map.get(args, :parent_cd)
    run_opts = %{worker_sup: Registry.worker_sup(pipeline_id), pipeline_id: pipeline_id, cd: parent_cd}
    effective_cd = Utils.resolve_cd(group.cd, ctx, run_opts) || parent_cd

    {steps_to_run, steps_to_skip} = Enum.split_with(group.steps, &Utils.eval_condition(Map.get(&1, :if), ctx))

    state = %{
      id: group.id,
      name: group.name,
      pipeline_id: pipeline_id,
      ctx: ctx,
      cd: effective_cd,
      timeout: group.timeout,
      pending: MapSet.new(steps_to_run, & &1.id),
      ctx_ops: [],
      timer_ref: nil
    }

    for step <- steps_to_skip do
      skip_data = %{id: step.id, name: step.name, pipeline_id: pipeline_id, group_id: group.id, now_ms: Utils.now_ms()}
      Output.Server.emit({:step_skipped, pipeline_id, skip_data})
    end

    {:ok, state, {:continue, {:start_steps, steps_to_run}}}
  end

  @impl true
  def handle_continue({:start_steps, []}, state) do
    skip_group(state)
  end

  @impl true
  def handle_continue({:start_steps, steps}, state) do
    event_data = %{id: state.id, name: state.name, step_count: length(steps), now_ms: Utils.now_ms()}
    Output.Server.emit({:group_started, state.pipeline_id, event_data})

    worker_sup = Registry.worker_sup(state.pipeline_id)

    args = %{
      step: nil,
      pipeline_id: state.pipeline_id,
      ctx: state.ctx,
      group_id: state.id,
      parent_cd: state.cd
    }

    for step <- steps do
      {:ok, _} = DynamicSupervisor.start_child(worker_sup, {Step.Server, %{args | step: step}})
    end

    timer_ref = schedule_timeout(state.timeout)
    {:noreply, %{state | timer_ref: timer_ref}}
  end

  @impl true
  def handle_cast({:step_done, step_id, :ok, ctx_op}, state) do
    case validate_ctx_op(ctx_op) do
      {:ok, ops} ->
        pending = MapSet.delete(state.pending, step_id)
        state = %{state | pending: pending, ctx_ops: ops ++ state.ctx_ops}

        if MapSet.size(pending) == 0 do
          finish_group(state)
        else
          {:noreply, state}
        end

      {:error, reason} ->
        fail_group(state, {reason, step_id})
    end
  end

  @impl true
  def handle_cast({:step_done, _step_id, {:error, reason}, _ctx_op}, state) do
    fail_group(state, reason)
  end

  @impl true
  def handle_info(:group_timeout, state) do
    fail_group(state, :timeout)
  end

  defp validate_ctx_op({:ctx_set, _}), do: {:error, :ctx_set_not_allowed}
  defp validate_ctx_op({:ctx_add, _} = op), do: {:ok, [op]}
  defp validate_ctx_op(_), do: {:ok, []}

  defp skip_group(state) do
    cancel_timeout(state.timer_ref)
    pipeline = Registry.pipeline(state.pipeline_id)

    event_data = %{id: state.id, name: state.name, now_ms: Utils.now_ms()}
    Output.Server.emit({:group_skipped, state.pipeline_id, event_data})

    GenServer.cast(pipeline, {:step_done, state.id, :ok, nil})
    {:stop, :normal, state}
  end

  defp finish_group(state) do
    cancel_timeout(state.timer_ref)

    case merge_ctx_ops(state.ctx_ops) do
      {:ok, merged} ->
        pipeline = Registry.pipeline(state.pipeline_id)

        event_data = %{id: state.id, name: state.name, now_ms: Utils.now_ms()}
        Output.Server.emit({:group_finished, state.pipeline_id, event_data})

        GenServer.cast(pipeline, {:step_done, state.id, :ok, {:ctx_add, merged}})
        {:stop, :normal, state}

      {:error, reason} ->
        fail_group(state, reason)
    end
  end

  defp fail_group(state, reason) do
    cancel_timeout(state.timer_ref)
    pipeline = Registry.pipeline(state.pipeline_id)

    event_data = %{id: state.id, name: state.name, reason: reason, now_ms: Utils.now_ms()}
    Output.Server.emit({:group_failed, state.pipeline_id, event_data})

    GenServer.cast(pipeline, {:step_done, state.id, {:error, reason}, nil})
    {:stop, {:shutdown, :group_failed}, state}
  end

  defp merge_ctx_ops(ctx_ops) do
    additions = Enum.map(ctx_ops, fn {:ctx_add, map} -> map end)
    throw_on_conflict = fn key, _v1, _v2 -> throw({:ctx_conflict, key}) end

    merged = Enum.reduce(additions, %{}, &Map.merge(&2, &1, throw_on_conflict))
    {:ok, merged}
  catch
    {:ctx_conflict, key} -> {:error, {:ctx_conflict, key}}
  end

  defp schedule_timeout(nil), do: nil
  defp schedule_timeout(:infinity), do: nil
  defp schedule_timeout(ms) when is_integer(ms), do: Process.send_after(self(), :group_timeout, ms)

  defp cancel_timeout(nil), do: :ok
  defp cancel_timeout(ref), do: Process.cancel_timer(ref)
end
