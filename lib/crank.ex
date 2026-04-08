defmodule Crank do
  @moduledoc """
  """
  alias Crank.Pipeline

  def new() do
    Pipeline.new()
  end

  def run(%Pipeline{} = pipeline) do
    Pipeline.start_pipeline(pipeline)
  end
end
