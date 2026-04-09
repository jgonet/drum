defmodule Crank.Group do
  @enforce_keys [:id, :name, :steps]
  defstruct [:id, :name, :steps, cd: nil, timeout: nil, if: nil]

  def new(name, steps, opts \\ []) when is_list(steps) do
    %__MODULE__{
      id: make_ref(),
      name: name,
      steps: steps,
      cd: opts[:cd],
      timeout: opts[:timeout],
      if: opts[:if]
    }
  end
end
