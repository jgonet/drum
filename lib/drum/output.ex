defmodule Drum.Output do
  @moduledoc false
  @type event ::
          {type :: atom(), subject_id :: term(), data :: term}

  def default do
    cond do
      test_env?() ->
        {Drum.Output.Test, []}

      IO.ANSI.enabled?() ->
        {Drum.Output.Live, []}

      true ->
        {Drum.Output.Plain, []}
    end
  end

  def resolve(Drum.Output.Live, args) do
    if IO.ANSI.enabled?() do
      {Drum.Output.Live, args}
    else
      {Drum.Output.Plain, args}
    end
  end

  def resolve(mod, args), do: {mod, args}

  defp test_env? do
    Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() == :test
  end

  @callback init(opts :: keyword()) :: term()
  @callback handle_event(event :: event(), state :: term()) :: new_state :: term()
end
