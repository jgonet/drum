defmodule Crank.Pipeline.Supervisor do
  use Supervisor, restart: :temporary

  def start_link(%{pipeline: %Crank.Pipeline{}} = args) do
    Supervisor.start_link(__MODULE__, args)
  end

  @impl true
  def init(%{pipeline: %Crank.Pipeline{} = pipeline} = args) do
    worker_sup = Crank.Registry.worker_sup(pipeline.id)
    owner = Map.fetch!(args, :owner)

    pipeline_server_args = %{
      pipeline: pipeline,
      name: Crank.Registry.pipeline(pipeline.id),
      owner: owner
    }

    children = [
      {DynamicSupervisor, name: worker_sup, strategy: :one_for_one},
      Supervisor.child_spec({Crank.Pipeline.Server, pipeline_server_args}, significant: true)
    ]

    Supervisor.init(children, strategy: :one_for_all, auto_shutdown: :any_significant)
  end
end
