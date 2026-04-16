defmodule Drum.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    Drum.TmpDir.sweep()
    output = Drum.Output.default()

    children = [
      {Registry, keys: :unique, name: Drum.Registry},
      {Drum.Output.Server, mod: output},
      {DynamicSupervisor, name: Drum.PipelinesSup, strategy: :one_for_one},
      {Drum.Subscriptions.Supervisor, []}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one)
  end
end
