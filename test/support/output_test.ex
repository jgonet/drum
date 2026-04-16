defmodule Drum.Output.Test do
  @moduledoc false
  @behaviour Drum.Output

  @table :drum_test_events

  def subscribe(pipeline_id, pid) do
    :ets.insert(@table, {pipeline_id, pid})
  end

  def unsubscribe(pipeline_id) do
    :ets.delete(@table, pipeline_id)
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:bag, :public, :named_table])
    :ok
  end

  @impl true
  def handle_event(:terminate, state), do: state

  @impl true
  def handle_event(event, state) do
    pipeline_id = elem(event, 1)

    for {_id, pid} <- :ets.lookup(@table, pipeline_id) do
      send(pid, {:drum_event, event})
    end

    state
  end
end
