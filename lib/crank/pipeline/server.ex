defmodule Crank.Pipeline.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Registry, Step, Utils}

  def start_link(%{name: name} = args) do
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(%{pipeline: pipeline}) do
    state = %{
      items: pipeline.items,
      pipeline_id: pipeline.id
    }

    {:ok, state, {:continue, :run_next}}
  end

  @impl true
  def handle_continue(:run_next, %{items: []} = state) do
    event_data = %{now_ms: Utils.now_ms()}
    Output.Server.emit({:pipeline_finished, state.pipeline_id, event_data})
    {:stop, :normal, state}
  end

  @impl true
  def handle_continue(:run_next, %{items: [%Step{} = step | rest]} = state) do
    worker_sup = Registry.worker_sup(state.pipeline_id)
    step_server_args = %{step: step, pipeline_id: state.pipeline_id}
    {:ok, _} = DynamicSupervisor.start_child(worker_sup, {Crank.Step.Server, step_server_args})
    {:noreply, %{state | items: rest}}
  end

  # future: handle_continue for %Group{}

  @impl true
  def handle_cast({:step_done, _id, :ok}, state) do
    {:noreply, state, {:continue, :run_next}}
  end

  @impl true
  def handle_cast({:step_done, _id, {:error, reason}}, state) do
    event_data = %{reason: reason, now_ms: Utils.now_ms()}
    Output.Server.emit({:pipeline_failed, state.pipeline_id, event_data})

    {:stop, {:shutdown, :step_failed}, state}
  end
end
