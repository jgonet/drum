defmodule Drum.GroupTest do
  use ExUnit.Case, async: true
  import Drum.Test.PipelineHelpers

  describe "group/3" do
    test "group finishes when all steps finish" do
      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [Drum.step("s1", "echo a"), Drum.step("s2", "echo b")])
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(events, &match?({:group_started, ^pid, %{name: "g"}}, &1))
      assert Enum.any?(events, &match?({:group_finished, ^pid, %{name: "g"}}, &1))
      assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
    end

    test "steps in group have group_id in event data" do
      group =
        Drum.group("g")
        |> Drum.step("s1", "echo a")

      pipeline =
        Drum.new()
        |> Drum.group(group)

      [group] = pipeline.items
      {:ok, pid} = run_pipeline(pipeline)
      events = collect_events(pid)

      step_events = for {:step_started, ^pid, data} <- events, do: data
      assert Enum.all?(step_events, &(&1.group_id == group.id))
    end

    test "group ctx_add values are merged into ctx for the next step" do
      test_pid = self()

      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [
          Drum.step("s1", fn _ctx, _opts -> {:ctx_add, %{a: 1}} end),
          Drum.step("s2", fn _ctx, _opts -> {:ctx_add, %{b: 2}} end)
        ])
        |> Drum.step("check", fn ctx, _opts -> send(test_pid, {:ctx, ctx}) end)
        |> run_pipeline()

      assert_receive {:ctx, ctx}
      assert ctx.a == 1
      assert ctx.b == 2
      assert {:ok, _} = await_pipeline(pid)
    end

    test "group fails if any step fails" do
      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [Drum.step("s1", "echo ok"), Drum.step("s2", "exit 1")])
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(events, &match?({:group_failed, ^pid, %{name: "g"}}, &1))
      assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
    end

    test "group fails if a step returns ctx_set" do
      step = Drum.step("s1", fn _ctx, _opts -> {:ctx_set, %{x: 1}} end)

      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [step])
        |> run_pipeline()

      events = collect_events(pid)
      step_id = step.id

      assert Enum.any?(
               events,
               &match?({:group_failed, ^pid, %{reason: {:ctx_set_not_allowed, ^step_id}}}, &1)
             )
    end

    test "group fails if two steps return the same ctx_add key" do
      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [
          Drum.step("s1", fn _ctx, _opts -> {:ctx_add, %{key: 1}} end),
          Drum.step("s2", fn _ctx, _opts -> {:ctx_add, %{key: 2}} end)
        ])
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(events, &match?({:group_failed, ^pid, _}, &1))
    end

    test "group ctx_add key conflicts with existing pipeline ctx fails the pipeline" do
      {:ok, pid} =
        Drum.new(%{key: :original})
        |> Drum.group("g", [Drum.step("s1", fn _ctx, _opts -> {:ctx_add, %{key: :new}} end)])
        |> run_pipeline()

      assert {:error, _} = await_pipeline(pid)
    end
  end
end
