defmodule Crank.Pipeline.Server do
  use GenServer, restart: :temporary
  alias Crank.{Group, Output, Registry, Step, Utils}

  def start_link(%{name: name} = args) do
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(%{pipeline: pipeline}) do
    state = %{
      items: pipeline.items,
      pipeline_id: pipeline.id,
      ctx: pipeline.ctx,
      cd: pipeline.cd
    }

    event_data = %{now_ms: Utils.now_ms(), items: summarize_items(pipeline.items)}
    Output.Server.emit({:pipeline_started, pipeline.id, event_data})
    {:ok, state, {:continue, :run_next}}
  end

  @impl true
  def handle_continue(:run_next, %{items: []} = state) do
    event_data = %{now_ms: Utils.now_ms()}
    Output.Server.emit({:pipeline_finished, state.pipeline_id, event_data})
    {:stop, :normal, state}
  end

  @impl true
  def handle_continue(:run_next, %{items: [item | rest]} = state) do
    if Utils.eval_condition(Map.get(item, :if), state.ctx) do
      start_item(item, rest, state)
    else
      emit_skipped(item, state)
      {:noreply, %{state | items: rest}, {:continue, :run_next}}
    end
  end

  @impl true
  def handle_cast({:step_done, _id, :ok, ctx_op}, state) do
    case apply_ctx_op(state.ctx, ctx_op) do
      {:ok, new_ctx} ->
        {:noreply, %{state | ctx: new_ctx}, {:continue, :run_next}}

      {:error, reason} ->
        event_data = %{reason: reason, now_ms: Utils.now_ms()}
        Output.Server.emit({:pipeline_failed, state.pipeline_id, event_data})
        {:stop, {:shutdown, :ctx_conflict}, state}
    end
  end

  @impl true
  def handle_cast({:step_done, _id, {:error, reason}, _ctx_op}, state) do
    event_data = %{reason: reason, now_ms: Utils.now_ms()}
    Output.Server.emit({:pipeline_failed, state.pipeline_id, event_data})

    {:stop, {:shutdown, :step_failed}, state}
  end

  defp start_item(%Step{} = step, rest, state) do
    worker_sup = Registry.worker_sup(state.pipeline_id)
    step_server_args = %{step: step, pipeline_id: state.pipeline_id, ctx: state.ctx, parent_cd: state.cd}
    {:ok, _} = DynamicSupervisor.start_child(worker_sup, {Crank.Step.Server, step_server_args})
    {:noreply, %{state | items: rest}}
  end

  defp start_item(%Group{} = group, rest, state) do
    worker_sup = Registry.worker_sup(state.pipeline_id)
    name = Registry.group(state.pipeline_id, group.id)
    group_server_args = %{group: group, pipeline_id: state.pipeline_id, ctx: state.ctx, name: name, parent_cd: state.cd}
    {:ok, _} = DynamicSupervisor.start_child(worker_sup, {Crank.Group.Server, group_server_args})
    {:noreply, %{state | items: rest}}
  end

  defp emit_skipped(%Step{} = step, state) do
    event_data = %{id: step.id, name: step.name, pipeline_id: state.pipeline_id, group_id: nil, now_ms: Utils.now_ms()}
    Output.Server.emit({:step_skipped, state.pipeline_id, event_data})
  end

  defp emit_skipped(%Group{} = group, state) do
    event_data = %{id: group.id, name: group.name, now_ms: Utils.now_ms()}
    Output.Server.emit({:group_skipped, state.pipeline_id, event_data})
  end

  defp summarize_items(items), do: Enum.map(items, &summarize_item/1)
  defp summarize_item(%Step{} = s), do: %{type: :step, id: s.id, name: s.name}
  defp summarize_item(%Group{} = g), do: %{type: :group, id: g.id, name: g.name, steps: summarize_items(g.steps)}

  defp apply_ctx_op(ctx, nil), do: {:ok, ctx}
  defp apply_ctx_op(ctx, {:ctx_set, new_ctx}) when is_map(new_ctx) do
    {:ok, Map.merge(new_ctx, Map.take(ctx, [:raw]))}
  end

  defp apply_ctx_op(ctx, {:ctx_add, additions}) when is_map(additions) do
    conflicts = additions |> Map.keys() |> Enum.filter(&Map.has_key?(ctx, &1))

    case conflicts do
      [] -> {:ok, Map.merge(ctx, additions)}
      _ -> {:error, {:ctx_conflict, conflicts}}
    end
  end
end
