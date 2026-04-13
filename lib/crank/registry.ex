defmodule Crank.Registry do
  def pipeline(pipeline_id), do: {:via, Registry, {__MODULE__, {:pipeline, pipeline_id}}}
  def worker_sup(pipeline_id), do: {:via, Registry, {__MODULE__, {:worker_sup, pipeline_id}}}

  def group(pipeline_id, group_id),
    do: {:via, Registry, {__MODULE__, {:group, pipeline_id, group_id}}}
end
