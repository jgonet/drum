defmodule Crank.Step.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Registry, Utils}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(%{step: step, pipeline_id: pipeline_id, ctx: ctx} = args) do
    group_id = Map.get(args, :group_id)

    state = %{
      id: step.id,
      name: step.name,
      action: step.action,
      pipeline_id: pipeline_id,
      group_id: group_id,
      ctx: ctx
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
      step_id: state.id
    }

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

    {:noreply, state}
  end

  @impl true
  def handle_info({:action_done, :ok, ctx_op}, state) do
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
    event_data = %{
      id: state.id,
      name: state.name,
      pipeline_id: state.pipeline_id,
      group_id: state.group_id,
      reason: reason,
      now_ms: Utils.now_ms()
    }

    Output.Server.emit({:step_failed, state.pipeline_id, event_data})
    notify_done(state, {:error, reason}, nil)
    {:stop, {:shutdown, :action_failed}, state}
  end

  defp notify_done(%{group_id: nil} = state, result, ctx_op) do
    pipeline = Registry.pipeline(state.pipeline_id)
    GenServer.cast(pipeline, {:step_done, state.id, result, ctx_op})
  end

  defp notify_done(state, result, ctx_op) do
    group = Registry.group(state.pipeline_id, state.group_id)
    GenServer.cast(group, {:step_done, state.id, result, ctx_op})
  end
end
