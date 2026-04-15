defmodule Crank.TimeoutTest do
  use ExUnit.Case, async: true
  import Crank.Test.PipelineHelpers

  describe "timeout option" do
    test "step timeout" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("s1", fn _ctx, _opts -> Process.sleep(5_000) end, timeout: 50)
        |> run_pipeline()

      events = collect_events(pid, 500)
      assert Enum.any?(events, &match?({:step_failed, ^pid, %{reason: :timeout}}, &1))
      assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
    end

    test "group timeout" do
      {:ok, pid} =
        Crank.new()
        |> Crank.group("g", [Crank.step("s1", fn _ctx, _opts -> Process.sleep(5_000) end)],
          timeout: 50
        )
        |> run_pipeline()

      events = collect_events(pid, 500)
      assert Enum.any?(events, &match?({:group_failed, ^pid, %{reason: :timeout}}, &1))
      assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
    end
  end
end
