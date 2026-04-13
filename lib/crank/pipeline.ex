defmodule Crank.Pipeline do
  @enforce_keys [:id]
  defstruct [:id, items: [], ctx: %{}, cd: nil]

  def new(ctx \\ %{}, opts \\ []) when is_map(ctx) do
    %__MODULE__{id: make_ref(), ctx: ctx, cd: opts[:cd]}
  end

  def add(%__MODULE__{} = pipeline, item) do
    %{pipeline | items: pipeline.items ++ [item]}
  end

  def start_pipeline(%__MODULE__{} = pipeline, opts \\ []) when is_list(opts) do
    owner = Keyword.get(opts, :owner, self())
    child_spec = {Crank.Pipeline.Supervisor, %{pipeline: pipeline, owner: owner}}
    {:ok, _} = DynamicSupervisor.start_child(Crank.PipelinesSup, child_spec)
    {:ok, pipeline.id}
  end
end
