defmodule Drum.Pipeline.Supervisor do
  use Supervisor, restart: :temporary

  def start_link(%{pipeline: %Drum.Pipeline{}} = args) do
    Supervisor.start_link(__MODULE__, args)
  end

  @impl true
  def init(%{pipeline: %Drum.Pipeline{} = pipeline} = args) do
    worker_sup = Drum.Registry.worker_sup(pipeline.id)
    owner = Map.fetch!(args, :owner)

    pipeline_server_args = %{
      pipeline: pipeline,
      name: Drum.Registry.pipeline(pipeline.id),
      owner: owner
    }

    children = [
      {DynamicSupervisor, name: worker_sup, strategy: :one_for_one},
      Supervisor.child_spec({Drum.Pipeline.Server, pipeline_server_args}, significant: true)
    ]

    Supervisor.init(children, strategy: :one_for_all, auto_shutdown: :any_significant)
  end
end
