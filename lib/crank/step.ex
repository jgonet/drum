defmodule Crank.Step do
  alias Crank.{Command, Pipeline}

  @enforce_keys [:id, :name, :commands]
  defstruct [:id, :name, commands: []]

  def new(%Pipeline{} = pipeline, name, cmd) when is_binary(cmd) do
    add(pipeline, name, [Command.new(cmd)])
  end

  def new(%Pipeline{} = pipeline, name, cmds) when is_list(cmds) do
    unless Enum.all?(cmds, &is_binary/1) do
      raise ArgumentError, "command list must contain only strings, found: #{inspect(cmds)}"
    end

    add(pipeline, name, Enum.map(cmds, &Command.new/1))
  end

  defp add(%Pipeline{} = pipeline, name, commands) do
    step = %__MODULE__{id: make_ref(), name: name, commands: commands}
    %{pipeline | items: pipeline.items ++ [step]}
  end
end
