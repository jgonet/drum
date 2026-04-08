defmodule Crank.Output do
  @moduledoc false
  @type event ::
          {type :: atom(), pipeline_id :: reference(), data :: term}

  @callback init(opts :: keyword()) :: term()
  @callback handle_event(event :: event(), state :: term()) :: new_state :: term()
end
