defmodule Crank do
  @moduledoc """
  """
  alias Crank.{Pipeline, Command, Group}

  defmacro script_dir do
    __CALLER__.file |> Path.dirname() |> Path.expand()
  end

  def new(ctx \\ %{}, opts \\ []) when is_map(ctx), do: Pipeline.new(ctx, opts)

  @doc """
  Merges env sources in order (later sources win). Accepts path strings and maps.
  Raises if any path string does not exist.
  """
  def source_env(sources) when is_list(sources) do
    Dotenvy.source!(sources, require_files: true, side_effect: nil)
  end

  def run(%Pipeline{} = pipeline), do: Pipeline.start_pipeline(pipeline)

  def group(%Pipeline{} = pipeline, name, steps, opts \\ []) when is_list(steps) do
    Pipeline.add(pipeline, Group.new(name, steps, opts))
  end

  def tmp_dir!(run_opts, mode) when mode in [:transient] do
    Crank.TmpDir.Server.create(run_opts.pipeline_id, mode)
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
          {:error, {:exit_code, code}} -> raise Crank.CommandError, exit_code: code, cmd: cmd
          {:error, reason} -> raise "command failed: #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "command failed to start: #{inspect(reason)}"
    end
  end
end
