defmodule Crank.Group do
  @enforce_keys [:id, :name, :steps]
  defstruct [:id, :name, :steps]

  def new(name, steps) when is_list(steps) do
    %__MODULE__{id: make_ref(), name: name, steps: steps}
  end
end
