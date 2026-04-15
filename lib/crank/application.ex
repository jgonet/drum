defmodule Crank.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    Crank.TmpDir.sweep()
    output = Crank.Output.default()

    children = [
      {Registry, keys: :unique, name: Crank.Registry},
      {Crank.Output.Server, mod: output},
      {DynamicSupervisor, name: Crank.PipelinesSup, strategy: :one_for_one},
      {Crank.Subscriptions.Supervisor, []}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one)
  end
end
