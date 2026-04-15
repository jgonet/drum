defmodule Crank do
  @moduledoc """
  """
  alias Crank.{Command, Group, Pipeline, Registry, Step, Utils}
  alias Crank.Subscriptions.{Dispatcher, PathSpec}

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

  def run(%Pipeline{} = pipeline) do
    {pipeline, subscriber_pid} = prepare_pipeline_for_run(pipeline)
    {:ok, pipeline_id} = Pipeline.start_pipeline(pipeline, owner: self())
    maybe_send_pipeline_id(subscriber_pid, pipeline_id)
    pipeline_id
  end

  def await(pipeline_id_or_ids, timeout \\ 5_000)

  def await(pipeline_ids, timeout) when is_list(pipeline_ids) do
    await_many(pipeline_ids, timeout)
  end

  def await(pipeline_id, :infinity) when is_reference(pipeline_id) do
    receive_pipeline_result(pipeline_id, :infinity)
  end

  def await(pipeline_id, timeout) when is_reference(pipeline_id) do
    receive_pipeline_result(pipeline_id, timeout)
  end

  def stop(pipeline_id, :graceful) when is_reference(pipeline_id) do
    case pipeline_stop(pipeline_id) do
      :ok -> :ok
      {:error, :noproc} -> :ok
    end
  end

  defdelegate with_cache(run_opts, name, key, opts), to: Crank.Cache

  def notify(signal), do: Dispatcher.notify(signal)

  def watch(paths) do
    validate_watch_paths!(paths)
    Dispatcher.watch(paths)
  rescue
    error in GlobEx.CompileError ->
      raise ArgumentError,
            "invalid watch pattern #{inspect(error.input)}: #{Exception.message(error)}"
  end

  def unwatch(watch_ref) when is_reference(watch_ref) do
    Dispatcher.unwatch(watch_ref)
  end

  def subscribe(name, callback, opts \\ []) when is_function(callback, 2),
    do: Dispatcher.subscribe(name, callback, opts)

  def unsubscribe(subscription_ref) when is_reference(subscription_ref) do
    Dispatcher.unsubscribe(subscription_ref)
  end

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

  defp await_many(pipeline_ids, :infinity) do
    Enum.map(pipeline_ids, &await(&1, :infinity))
  end

  defp await_many(pipeline_ids, timeout) when is_integer(timeout) and timeout >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout

    pipeline_ids
    |> Utils.reduce_ok([], fn pipeline_id, acc ->
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining < 0 do
        {:error, :timeout}
      else
        case await(pipeline_id, remaining) do
          {:error, :timeout} -> {:error, :timeout}
          result -> {:ok, [result | acc]}
        end
      end
    end)
    |> case do
      {:ok, values} -> Enum.reverse(values)
      {:error, :timeout} = error -> error
    end
  end

  defp pipeline_stop(pipeline_id) do
    GenServer.call(Registry.pipeline(pipeline_id), {:stop, :graceful})
  catch
    :exit, _reason -> {:error, :noproc}
  end

  defp receive_pipeline_result(pipeline_id, :infinity) do
    receive do
      {:crank_pipeline_result, ^pipeline_id, result} -> result
    end
  end

  defp receive_pipeline_result(pipeline_id, timeout)
       when is_integer(timeout) and timeout >= 0 do
    receive do
      {:crank_pipeline_result, ^pipeline_id, result} -> result
    after
      timeout -> {:error, :timeout}
    end
  end

  defp prepare_pipeline_for_run(%Pipeline{} = pipeline) do
    case Elixir.Registry.lookup(Crank.Subscriptions.RunRegistry, self()) do
      [{_owner, {subscriber_pid, run_meta}}] ->
        next_pipeline = %{pipeline | meta: Map.merge(pipeline.meta, run_meta)}
        {next_pipeline, subscriber_pid}

      [] ->
        {pipeline, nil}
    end
  end

  defp maybe_send_pipeline_id(nil, _pipeline_id), do: :ok

  defp maybe_send_pipeline_id(subscriber_pid, pipeline_id) do
    send(subscriber_pid, {:subscription, :pipeline_id, self(), pipeline_id})
    :ok
  end

  defp validate_watch_paths!(path) when is_binary(path) do
    validate_watch_path!(path)
  end

  defp validate_watch_paths!(paths) when is_list(paths) do
    Enum.each(paths, &validate_watch_path!/1)
  end

  defp validate_watch_paths!(_paths), do: :ok

  defp validate_watch_path!(path) when is_binary(path) do
    expanded_path = Path.expand(path)

    if not PathSpec.valid?(path) do
      suggested_children = Path.join(expanded_path, "*")
      suggested_recursive = Path.join(expanded_path, "**")

      raise ArgumentError,
            "ambiguous watch path #{inspect(path)}: use #{inspect(suggested_children)} or #{inspect(suggested_recursive)}"
    end
  end
end
