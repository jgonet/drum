defmodule Crank.Group.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Registry, Step, Utils}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args.name)
  end

  @impl true
  def init(%{group: group, pipeline_id: pipeline_id, ctx: ctx}) do
    state = %{
      id: group.id,
      name: group.name,
      pipeline_id: pipeline_id,
      ctx: ctx,
      pending: MapSet.new(group.steps, & &1.id),
      ctx_ops: []
    }

    event_data = %{id: group.id, name: group.name, now_ms: Utils.now_ms()}
    Output.Server.emit({:group_started, pipeline_id, event_data})
    {:ok, state, {:continue, {:start_steps, group.steps}}}
  end

  @impl true
  def handle_continue({:start_steps, steps}, state) do
    worker_sup = Registry.worker_sup(state.pipeline_id)

    args = %{
      step: nil,
      pipeline_id: state.pipeline_id,
      ctx: state.ctx,
      group_id: state.id
    }

    for step <- steps do
      {:ok, _} = DynamicSupervisor.start_child(worker_sup, {Step.Server, %{args | step: step}})
    end

    {:noreply, state}
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
        fail_group(state, reason)
    end
  end

  @impl true
  def handle_cast({:step_done, _step_id, {:error, reason}, _ctx_op}, state) do
    fail_group(state, reason)
  end

  defp validate_ctx_op({:ctx_set, _}), do: {:error, :bad_return}
  defp validate_ctx_op({:ctx_add, _} = op), do: {:ok, [op]}
  defp validate_ctx_op(_), do: {:ok, []}

  defp finish_group(state) do
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
end
