defmodule Crank do
  @moduledoc """
  """
  alias Crank.Pipeline
  alias Crank.Config

  def new({argv, env}, %{} = config, _opts \\ []) when is_list(argv) and is_map(env) do
    pipeline =
      {argv, env}
      |> Config.to_context!(config)
      |> Pipeline.new()

    {:ok, pipeline}
  end
end
