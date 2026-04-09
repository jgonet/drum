defmodule Crank.Command do
  @enforce_keys [:id, :cmd]
  defstruct [:id, :cmd]

  def new(cmd) when is_binary(cmd) do
    %__MODULE__{id: make_ref(), cmd: cmd}
  end

  def start(%__MODULE__{} = command, opts) do
    command_server_args = %{
      id: command.id,
      cmd: command.cmd,
      notify: opts.notify,
      pipeline_id: opts.pipeline_id,
      step_id: opts.step_id,
      cd: Map.get(opts, :cd)
    }

    DynamicSupervisor.start_child(opts.worker_sup, {Crank.Command.Server, command_server_args})
  end
end
