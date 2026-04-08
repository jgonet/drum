defmodule Crank.Pipeline do
  alias Crank.Step

  @enforce_keys [:id]
  defstruct [:id, items: [], ctx: %{}]

  def new(ctx \\ %{}) when is_map(ctx) do
    %__MODULE__{id: make_ref(), ctx: ctx}
  end

  def add(%__MODULE__{} = pipeline, %Step{} = step) do
    %{pipeline | items: pipeline.items ++ [step]}
  end

  def start_pipeline(%__MODULE__{} = pipeline) do
    child_spec = {Crank.Pipeline.Supervisor, pipeline}
    {:ok, _} = DynamicSupervisor.start_child(Crank.PipelinesSup, child_spec)
    {:ok, pipeline.id}
  end
end
