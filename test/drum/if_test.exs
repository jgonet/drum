defmodule Drum.IfTest do
  use ExUnit.Case, async: true
  import Drum.Test.PipelineHelpers

  describe "step if:" do
    test "step if: boolean" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: false)
        |> Drum.step("s2", fn _ctx, _opts -> send(test_pid, :done) end)
        |> run_pipeline()

      events = collect_events(pid)
      assert_receive :done
      refute_received :ran
      assert Enum.any?(events, &match?({:step_skipped, ^pid, %{name: "s1"}}, &1))
      assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))

      {:ok, pid2} =
        Drum.new()
        |> Drum.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: true)
        |> run_pipeline()

      assert {:ok, _} = await_pipeline(pid2)
      assert_receive :ran
    end

    test "step if: atom" do
      test_pid = self()

      {:ok, pid} =
        Drum.new(%{run_it: true})
        |> Drum.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: :run_it)
        |> run_pipeline()

      assert {:ok, _} = await_pipeline(pid)
      assert_receive :ran

      {:ok, pid2} =
        Drum.new(%{run_it: false})
        |> Drum.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: :run_it)
        |> Drum.step("s2", fn _ctx, _opts -> send(test_pid, :done) end)
        |> run_pipeline()

      assert {:ok, _} = await_pipeline(pid2)
      assert_receive :done
      refute_received :ran
    end
  end

  describe "group if:" do
    test "group if: false skips" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [Drum.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end)],
          if: false
        )
        |> Drum.step("s2", fn _ctx, _opts -> send(test_pid, :done) end)
        |> run_pipeline()

      events = collect_events(pid)
      assert_receive :done
      refute_received :ran
      assert Enum.any?(events, &match?({:group_skipped, ^pid, %{name: "g"}}, &1))
      assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
    end

    test "group step if: false skips, group finishes" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [
          Drum.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: false),
          Drum.step("s2", fn _ctx, _opts -> send(test_pid, :other) end)
        ])
        |> run_pipeline()

      events = collect_events(pid)
      assert_receive :other
      refute_received :ran
      assert Enum.any?(events, &match?({:step_skipped, ^pid, %{name: "s1"}}, &1))
      assert Enum.any?(events, &match?({:group_finished, ^pid, _}, &1))
    end

    test "group all steps skipped is marked skipped" do
      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [Drum.step("s1", "echo hi", if: false)])
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(events, &match?({:group_skipped, ^pid, %{name: "g"}}, &1))
      refute Enum.any?(events, &match?({:group_finished, ^pid, _}, &1))
      assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
    end
  end
end
