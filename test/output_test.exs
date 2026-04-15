defmodule Crank.OutputTest do
  use ExUnit.Case, async: true

  test "default/0 uses test adapter in test env" do
    assert {Crank.Output.Test, []} = Crank.Output.default()
  end
end
