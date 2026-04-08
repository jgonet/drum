defmodule Crank.Output.Plain do
  @moduledoc false
  @behaviour Crank.Output

  @impl true
  def init(_opts) do
    nil
  end

  @impl true
  def handle_event(event, _state) do
    IO.inspect(event)
  end
end
