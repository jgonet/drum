defmodule Crank.Subscriptions.SubscriptionOpts do
  @known_options [:base_context, :rerun, :on_failure]

  def normalize!(opts) when is_list(opts) do
    ensure_known_option_keys!(opts)

    base_context = Keyword.get(opts, :base_context, %{})
    rerun = Keyword.get(opts, :rerun, :wait)
    on_failure = Keyword.get(opts, :on_failure, :drop)

    if not is_map(base_context) do
      raise ArgumentError, "expected :base_context to be a map, got: #{inspect(base_context)}"
    end

    if rerun not in [:wait, {:kill, :graceful}] do
      raise ArgumentError,
            "expected :rerun to be :wait or {:kill, :graceful}, got: #{inspect(rerun)}"
    end

    if on_failure not in [:drop, :continue] do
      raise ArgumentError,
            "expected :on_failure to be :drop or :continue, got: #{inspect(on_failure)}"
    end

    %{base_context: base_context, on_failure: on_failure, rerun: rerun}
  end

  def normalize!(opts) do
    raise ArgumentError, "expected subscription opts to be a keyword list, got: #{inspect(opts)}"
  end

  defp ensure_known_option_keys!(opts) do
    unknown_keys =
      opts
      |> Keyword.keys()
      |> Enum.reject(&(&1 in @known_options))

    if unknown_keys != [] do
      raise ArgumentError, "unsupported subscribe options: #{inspect(unknown_keys)}"
    end
  end
end
