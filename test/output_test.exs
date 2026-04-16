defmodule Drum.OutputTest do
  use ExUnit.Case, async: true

  test "default/0 uses test adapter in test env" do
    assert {Drum.Output.Test, []} = Drum.Output.default()
  end
end
