defmodule Crank.Step do
  @enforce_keys [:id, :name, :action]
  defstruct [:id, :name, :action]

  def new(name, cmd) when is_binary(cmd) do
    %__MODULE__{id: make_ref(), name: name, action: fn _ctx, cmd_opts -> Crank.cmd!(cmd, cmd_opts) end}
  end

  def new(name, cmds) when is_list(cmds) do
    if not Enum.all?(cmds, &is_binary/1) do
      raise ArgumentError, "command list must contain only strings, found: #{inspect(cmds)}"
    end

    %__MODULE__{id: make_ref(), name: name, action: fn _ctx, cmd_opts -> Enum.each(cmds, &Crank.cmd!(&1, cmd_opts)) end}
  end

  def new(name, action) when is_function(action, 2) do
    %__MODULE__{id: make_ref(), name: name, action: action}
  end
end
