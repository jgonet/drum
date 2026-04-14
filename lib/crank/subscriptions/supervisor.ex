defmodule Crank.Subscriptions.Supervisor do
  use Supervisor

  @run_supervisor Crank.Subscriptions.RunSupervisor
  @subscriber_supervisor Crank.Subscriptions.SubscriberSupervisor
  @watch_supervisor Crank.Subscriptions.WatchSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Crank.Subscriptions.SubscriberRegistry},
      {Registry, keys: :unique, name: Crank.Subscriptions.WatcherRegistry},
      {Registry, keys: :unique, name: Crank.Subscriptions.RunRegistry},
      {DynamicSupervisor, name: @run_supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: @subscriber_supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: @watch_supervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
