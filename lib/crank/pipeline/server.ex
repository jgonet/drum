defmodule Crank.Pipeline.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Step, Utils}

  def start_link(%{name: name} = args) do
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(%{pipeline: pipeline, worker_sup: worker_sup}) do
    state = %{
      items: pipeline.items,
      pipeline_id: pipeline.id,
      worker_sup: worker_sup
    }

    {:ok, state, {:continue, :run_next}}
  end

  @impl true
  def handle_continue(:run_next, %{items: []} = state) do
    Output.Server.emit({:pipeline_finished, state.pipeline_id, %{now_ms: Utils.now_ms()}})
    {:stop, :normal, state}
  end

  @impl true
  def handle_continue(:run_next, %{items: [%Step{} = step | rest]} = state) do
    step_server_args = %{
      step: step,
      pipeline_id: state.pipeline_id,
      notify: self(),
      worker_sup: state.worker_sup
    }

    {:ok, _} =
      DynamicSupervisor.start_child(state.worker_sup, {Crank.Step.Server, step_server_args})

    {:noreply, %{state | items: rest}}
  end

  # future: handle_continue for %Group{}

  @impl true
  def handle_info({:step_done, _id, :ok}, state) do
    {:noreply, state, {:continue, :run_next}}
  end

  @impl true
  def handle_info({:step_done, _id, {:error, reason}}, state) do
    event_data = %{reason: reason, now_ms: Utils.now_ms()}
    Output.Server.emit({:pipeline_failed, state.pipeline_id, event_data})
    {:stop, {:shutdown, :step_failed}, state}
  end
end
