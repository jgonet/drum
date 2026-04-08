defmodule Crank.Step.Server do
  use GenServer, restart: :temporary
  alias Crank.{Command, Output, Utils}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(%{step: step, pipeline_id: pipeline_id, notify: notify, worker_sup: worker_sup}) do
    state = %{
      id: step.id,
      name: step.name,
      commands: step.commands,
      pipeline_id: pipeline_id,
      notify: notify,
      worker_sup: worker_sup
    }

    Output.Server.emit({:step_started, pipeline_id, %{id: step.id, name: step.name, pipeline_id: pipeline_id, now_ms: Utils.now_ms()}})

    {:ok, state, {:continue, :run_next}}
  end

  @impl true
  def handle_continue(:run_next, %{commands: []} = state) do
    Output.Server.emit({:step_finished, state.pipeline_id, %{id: state.id, name: state.name, pipeline_id: state.pipeline_id, now_ms: Utils.now_ms()}})
    send(state.notify, {:step_done, state.id, :ok})
    {:stop, :normal, state}
  end

  @impl true
  def handle_continue(:run_next, %{commands: [cmd | rest]} = state) do
    case Command.start(cmd, %{worker_sup: state.worker_sup, notify: self(), pipeline_id: state.pipeline_id, step_id: state.id}) do
      {:ok, _} ->
        {:noreply, %{state | commands: rest}}

      {:error, reason} ->
        Output.Server.emit({:step_failed, state.pipeline_id, %{id: state.id, name: state.name, pipeline_id: state.pipeline_id, reason: reason, now_ms: Utils.now_ms()}})
        send(state.notify, {:step_done, state.id, {:error, reason}})
        {:stop, {:shutdown, :command_start_failed}, state}
    end
  end

  @impl true
  def handle_info({:command_done, _id, :ok}, state) do
    {:noreply, state, {:continue, :run_next}}
  end

  @impl true
  def handle_info({:command_done, _id, {:error, reason}}, state) do
    Output.Server.emit({:step_failed, state.pipeline_id, %{id: state.id, name: state.name, pipeline_id: state.pipeline_id, reason: reason, now_ms: Utils.now_ms()}})
    send(state.notify, {:step_done, state.id, {:error, reason}})
    {:stop, {:shutdown, :command_failed}, state}
  end
end
