defmodule Crank.Test.PipelineHelpers do
  def run_pipeline(%Crank.Pipeline{} = pipeline) do
    id = pipeline.id
    Crank.Output.Test.subscribe(id, self())
    ExUnit.Callbacks.on_exit(fn -> Crank.Output.Test.unsubscribe(id) end)
    {:ok, ^id} = Crank.run(pipeline)
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
end
