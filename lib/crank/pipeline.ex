defmodule Crank.Pipeline do
  @enforce_keys [:id]
  defstruct [:id, items: [], ctx: %{}]

  def new() do
    %__MODULE__{id: make_ref()}
  end

  def start_pipeline(%__MODULE__{} = pipeline) do
    child_spec = {Crank.Pipeline.Supervisor, pipeline}
    {:ok, _} = DynamicSupervisor.start_child(Crank.PipelinesSup, child_spec)
    {:ok, pipeline.id}
  end
end
