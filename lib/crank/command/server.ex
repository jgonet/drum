defmodule Crank.Command.Server do
  use GenServer, restart: :temporary
  alias Crank.{Output, Utils}

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    %{
      id: id,
      cmd: cmd,
      notify: notify,
      pipeline_id: pipeline_id,
      step_id: step_id,
      cd: cd
    } = args

    exec_opts = [:monitor, {:stdout, self()}, {:stderr, self()}]
    exec_opts = if cd, do: [{:cd, cd} | exec_opts], else: exec_opts

    case :exec.run(cmd, exec_opts) do
      {:ok, exec_pid, os_pid} ->
        event_data = %{
          id: id,
          cmd: cmd,
          step_id: step_id,
          pipeline_id: pipeline_id,
          now_ms: Utils.now_ms()
        }

        Output.Server.emit({:command_started, pipeline_id, event_data})

        {:ok,
         %{
           exec_pid: exec_pid,
           os_pid: os_pid,
           id: id,
           pipeline_id: pipeline_id,
           step_id: step_id,
           notify: notify
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info({:stdout, os_pid, data}, %{os_pid: os_pid} = state) do
    event_data = %{
      id: state.id,
      step_id: state.step_id,
      pipeline_id: state.pipeline_id,
      data: data,
      now_ms: Utils.now_ms()
    }

    Output.Server.emit({:command_stdout, state.pipeline_id, event_data})
    {:noreply, state}
  end

  @impl true
  def handle_info({:stderr, os_pid, data}, %{os_pid: os_pid} = state) do
    event_data = %{
      id: state.id,
      step_id: state.step_id,
      pipeline_id: state.pipeline_id,
      data: data,
      now_ms: Utils.now_ms()
    }

    Output.Server.emit({:command_stderr, state.pipeline_id, event_data})
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, os_pid, :process, _pid, exit_reason}, %{os_pid: os_pid} = state) do
    event_data = %{
      id: state.id,
      step_id: state.step_id,
      pipeline_id: state.pipeline_id,
      now_ms: Utils.now_ms()
    }

    case to_numeric_exit_code(exit_reason) do
      0 ->
        Output.Server.emit({:command_finished, state.pipeline_id, event_data})
        send(state.notify, {:command_done, state.id, :ok})

      code ->
        data = Map.put(event_data, :exit_code, code)
        Output.Server.emit({:command_failed, state.pipeline_id, data})

        send(state.notify, {:command_done, state.id, {:error, {:exit_code, code}}})
    end

    {:stop, :normal, state}
  end

  @impl true
  def terminate(_reason, state) do
    :exec.stop(state.exec_pid)
  end

  defp to_numeric_exit_code(:normal), do: 0

  defp to_numeric_exit_code({:exit_status, status}) do
    case :exec.status(status) do
      {:status, code} when is_integer(code) -> code
      {:signal, sig, _core} when is_integer(sig) -> 128 + sig
      {:signal, _sig, _core} -> 128
    end
  end

  defp to_numeric_exit_code(_), do: 1
end
