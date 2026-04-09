defmodule Crank do
  @moduledoc """
  """
  alias Crank.{Pipeline, Command, Group}

  defmacro script_dir do
    __CALLER__.file |> Path.dirname() |> Path.expand()
  end

  def new(ctx \\ %{}, opts \\ []) when is_map(ctx), do: Pipeline.new(ctx, opts)

  def run(%Pipeline{} = pipeline), do: Pipeline.start_pipeline(pipeline)

  def group(%Pipeline{} = pipeline, name, steps, opts \\ []) when is_list(steps) do
    Pipeline.add(pipeline, Group.new(name, steps, opts))
  end

  def cmd!(cmd, cmd_opts) when is_binary(cmd) do
    command = Command.new(cmd)

    id = command.id

    case Command.start(command, Map.put(cmd_opts, :notify, self())) do
      {:ok, server_pid} ->
        ref = Process.monitor(server_pid)

        result =
          receive do
            {:command_done, ^id, :ok} -> :ok
            {:command_done, ^id, {:error, reason}} -> {:error, reason}
            {:DOWN, ^ref, :process, _pid, reason} -> {:error, {:command_crashed, reason}}
          end

        Process.demonitor(ref, [:flush])

        case result do
          :ok -> :ok
          {:error, reason} -> raise "command failed: #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "command failed to start: #{inspect(reason)}"
    end
  end
end
