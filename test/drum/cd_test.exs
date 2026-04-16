defmodule Drum.CdTest do
  use ExUnit.Case, async: true
  import Drum.Test.PipelineHelpers

  describe "cd option" do
    test "pipeline cd" do
      {:ok, pid} =
        Drum.new(%{}, cd: "/tmp")
        |> Drum.step("s1", "pwd")
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "tmp"))
    end

    test "step cd overrides pipeline cd" do
      {:ok, pid} =
        Drum.new(%{}, cd: "/tmp")
        |> Drum.step("s1", "pwd", cd: "/var")
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "var"))
      refute Enum.any?(stdout_of(events), fn d -> String.trim(d) == "/tmp" end)
    end

    test "group cd inherited by steps" do
      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [Drum.step("s1", "pwd")], cd: "/tmp")
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "tmp"))
    end

    test "step cd overrides group cd" do
      {:ok, pid} =
        Drum.new()
        |> Drum.group("g", [Drum.step("s1", "pwd", cd: "/var")], cd: "/tmp")
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "var"))
      refute Enum.any?(stdout_of(events), fn d -> String.trim(d) == "/tmp" end)
    end

    test "cd: atom reads from ctx" do
      {:ok, pid} =
        Drum.new(%{work: "/tmp"})
        |> Drum.step("s1", "pwd", cd: :work)
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "tmp"))
    end

    test "cd: fn receives ctx and run_opts" do
      test_pid = self()

      {:ok, pid} =
        Drum.new(%{base: "/tmp"})
        |> Drum.step("s1", fn _ctx, _opts -> :ok end,
          cd: fn ctx, run_opts ->
            send(test_pid, {:args, ctx, run_opts})
            ctx.base
          end
        )
        |> run_pipeline()

      assert_receive {:args, ctx, run_opts}
      assert ctx.base == "/tmp"
      assert is_reference(run_opts.pipeline_id)
      assert {:ok, _} = await_pipeline(pid)
    end
  end
end
