defmodule Crank do
  @moduledoc """
  """
  alias Crank.{Pipeline, Command, Group, Step}

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

  def step(name, action), do: Step.new(name, action, [])

  def step(%Pipeline{} = pipeline, name, action) do
    Pipeline.add(pipeline, step(name, action))
  end

  def step(%Group{} = group, name, action) do
    Group.add(group, step(name, action))
  end

  def step(name, action, opts), do: Step.new(name, action, opts)

  def step(%Pipeline{} = pipeline, name, action, opts) do
    Pipeline.add(pipeline, step(name, action, opts))
  end

  def step(%Group{} = group, name, action, opts) do
    Group.add(group, step(name, action, opts))
  end

  def group(name), do: Group.new(name, [], [])

  def group(%Pipeline{} = pipeline, %Group{} = group), do: Pipeline.add(pipeline, group)

  def group(name, items) when is_list(items) do
    if Keyword.keyword?(items) do
      Group.new(name, [], items)
    else
      Group.new(name, items, [])
    end
  end

  def group(%Pipeline{} = pipeline, name, steps) when is_list(steps) do
    Pipeline.add(pipeline, group(name, steps))
  end

  def group(name, steps, opts) when is_list(steps), do: Group.new(name, steps, opts)

  def group(%Pipeline{} = pipeline, name, steps, opts) when is_list(steps) do
    Pipeline.add(pipeline, group(name, steps, opts))
  end

  def tmp_dir!(:transient) do
    {:ok, path} = Crank.TmpDir.create_transient()
    path
  end

  def tmp_dir!({:persistent, opts}) when is_list(opts) do
    key = Keyword.fetch!(opts, :key)
    ttl = Keyword.fetch!(opts, :ttl)
    {:ok, path} = Crank.TmpDir.create_persistent(key, ttl)
    path
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
