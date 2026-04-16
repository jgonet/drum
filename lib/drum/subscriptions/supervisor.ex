defmodule Drum.Subscriptions.Supervisor do
  use Supervisor

  @run_supervisor Drum.Subscriptions.RunSupervisor
  @subscriber_supervisor Drum.Subscriptions.SubscriberSupervisor
  @watch_supervisor Drum.Subscriptions.WatchSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Drum.Subscriptions.SubscriberRegistry},
      {Registry, keys: :unique, name: Drum.Subscriptions.WatcherRegistry},
      {Registry, keys: :unique, name: Drum.Subscriptions.RunRegistry},
      {DynamicSupervisor, name: @run_supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: @subscriber_supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: @watch_supervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
