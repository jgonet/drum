defmodule Crank.Registry do
  def pipeline(pipeline_id), do: {:via, Registry, {__MODULE__, {:pipeline, pipeline_id}}}
  def worker_sup(pipeline_id), do: {:via, Registry, {__MODULE__, {:worker_sup, pipeline_id}}}

  def group(pipeline_id, group_id),
    do: {:via, Registry, {__MODULE__, {:group, pipeline_id, group_id}}}

  def lookup_pipeline(pipeline_id) do
    case Registry.lookup(__MODULE__, {:pipeline, pipeline_id}) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end
end
