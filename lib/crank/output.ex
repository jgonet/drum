defmodule Crank.Output do
  @moduledoc false
  @type event ::
          {type :: atom(), subject_id :: term(), data :: term}

  def default do
    cond do
      test_env?() ->
        {Crank.Output.Test, []}

      IO.ANSI.enabled?() ->
        {Crank.Output.Live, []}

      true ->
        {Crank.Output.Plain, []}
    end
  end

  def resolve(Crank.Output.Live, args) do
    if IO.ANSI.enabled?() do
      {Crank.Output.Live, args}
    else
      {Crank.Output.Plain, args}
    end
  end

  def resolve(mod, args), do: {mod, args}

  defp test_env? do
    Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() == :test
  end

  @callback init(opts :: keyword()) :: term()
  @callback handle_event(event :: event(), state :: term()) :: new_state :: term()
end
