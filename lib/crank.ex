defmodule Crank do
  @moduledoc """
  """
  alias Crank.{Pipeline, Command}

  def new(ctx \\ %{}) when is_map(ctx), do: Pipeline.new(ctx)

  def run(%Pipeline{} = pipeline), do: Pipeline.start_pipeline(pipeline)

  def cmd!(cmd, cmd_opts) when is_binary(cmd) do
    command = Command.new(cmd)

    id = command.id

    case Command.start(command, Map.put(cmd_opts, :notify, self())) do
      {:ok, _} ->
        receive do
          {:command_done, ^id, :ok} -> :ok
          {:command_done, ^id, {:error, reason}} -> raise "command failed: #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "command failed to start: #{inspect(reason)}"
    end
  end
end
