defmodule Crank.Test.PipelineHelpers do
  import ExUnit.Assertions

  def run_pipeline(%Crank.Pipeline{} = pipeline) do
    id = pipeline.id
    Crank.Output.Test.subscribe(id, self())
    ExUnit.Callbacks.on_exit(fn -> Crank.Output.Test.unsubscribe(id) end)
    ^id = Crank.run(pipeline)
    {:ok, id}
  end

  def await_pipeline(pipeline_id, timeout \\ 5_000) do
    receive do
      {:crank_event, {:pipeline_finished, ^pipeline_id, data}} -> {:ok, data}
      {:crank_event, {:pipeline_failed, ^pipeline_id, data}} -> {:error, data}
    after
      timeout -> {:error, :timeout}
    end
  end

  def collect_events(pipeline_id, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_collect(pipeline_id, [], deadline)
  end

  defp do_collect(pipeline_id, acc, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      Enum.reverse(acc)
    else
      receive do
        {:crank_event, {:pipeline_finished, ^pipeline_id, _} = event} ->
          Enum.reverse([event | acc])

        {:crank_event, {:pipeline_failed, ^pipeline_id, _} = event} ->
          Enum.reverse([event | acc])

        {:crank_event, {_, ^pipeline_id, _} = event} ->
          do_collect(pipeline_id, [event | acc], deadline)
      after
        remaining -> Enum.reverse(acc)
      end
    end
  end

  # Event extraction helpers

  def stdout_of(events) do
    for {:command_stdout, _pid, %{data: d}} <- events, do: d
  end

  def stderr_of(events) do
    for {:command_stderr, _pid, %{data: d}} <- events, do: d
  end

  def command_events(events) do
    for {t, _, _} = event <- events, t in [:command_started, :command_finished], do: event
  end

  # Process lifecycle

  def assert_process_down(pid, timeout \\ 500) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, timeout
  end

  # Gated step — sends {tag, self()} to test_pid then blocks until release message.
  # opts: tag: (default :step_pid), result: (default :ok), release: (default :release)
  def gated_step(pipeline_or_name, name_or_pid, opts \\ [])

  def gated_step(%Crank.Pipeline{} = pipeline, name, test_pid) do
    Crank.Pipeline.add(pipeline, gated_step(name, test_pid, []))
  end

  def gated_step(name, test_pid, opts) do
    tag = Keyword.get(opts, :tag, :step_pid)
    result = Keyword.get(opts, :result, :ok)
    release = Keyword.get(opts, :release, :release)

    Crank.step(name, fn _ctx, _cmd_opts ->
      send(test_pid, {tag, self()})

      receive do
        ^release -> result
      end
    end)
  end

  def gated_step(%Crank.Pipeline{} = pipeline, name, test_pid, opts) do
    Crank.Pipeline.add(pipeline, gated_step(name, test_pid, opts))
  end

  # Cleanup helper — registers an on_exit to remove a directory
  def register_tmpdir_cleanup(dir) do
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(dir) end)
  end
end
