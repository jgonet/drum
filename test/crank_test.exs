defmodule CrankTest do
  use ExUnit.Case
  doctest Crank

  test "greets the world" do
    assert Crank.hello() == :world
  end
end
