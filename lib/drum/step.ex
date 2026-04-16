defmodule Drum.Step do
  @enforce_keys [:id, :name, :action]
  defstruct [:id, :name, :action, cd: nil, timeout: nil, if: nil]

  def new(name, action, opts \\ [])

  def new(name, cmd, opts) when is_binary(cmd) do
    %__MODULE__{
      id: make_ref(),
      name: name,
      action: fn _ctx, cmd_opts -> Drum.cmd!(cmd, cmd_opts) end,
      cd: opts[:cd],
      timeout: opts[:timeout],
      if: opts[:if]
    }
  end

  def new(name, cmds, opts) when is_list(cmds) do
    if not Enum.all?(cmds, &is_binary/1) do
      raise ArgumentError, "command list must contain only strings, found: #{inspect(cmds)}"
    end

    %__MODULE__{
      id: make_ref(),
      name: name,
      action: fn _ctx, cmd_opts -> Enum.each(cmds, &Drum.cmd!(&1, cmd_opts)) end,
      cd: opts[:cd],
      timeout: opts[:timeout],
      if: opts[:if]
    }
  end

  def new(name, action, opts) when is_function(action, 2) do
    %__MODULE__{
      id: make_ref(),
      name: name,
      action: action,
      cd: opts[:cd],
      timeout: opts[:timeout],
      if: opts[:if]
    }
  end
end
