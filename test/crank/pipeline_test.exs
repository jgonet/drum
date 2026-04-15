defmodule Crank.PipelineTest do
  use ExUnit.Case, async: true
  import Crank.Test.PipelineHelpers

  describe "step/3" do
    test "step" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "echo hello")
        |> run_pipeline()

      assert {:ok, _} = await_pipeline(pid)
    end

    test "multiple commands in step" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", ["echo a", "echo b"])
        |> run_pipeline()

      events = collect_events(pid)
      cmd_events = command_events(events)

      assert [
               {:command_started, ^pid, %{cmd: "echo a"}},
               {:command_finished, ^pid, _},
               {:command_started, ^pid, %{cmd: "echo b"}},
               {:command_finished, ^pid, _}
             ] = cmd_events
    end

    test "multiple sequential steps" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "echo first")
        |> Crank.step("step2", "echo second")
        |> run_pipeline()

      events = collect_events(pid)

      step1_finish_idx = Enum.find_index(events, &match?({:command_finished, _, _}, &1))

      step2_start_idx =
        Enum.find_index(events, fn
          {:command_started, _, %{cmd: "echo second"}} -> true
          _ -> false
        end)

      assert step1_finish_idx < step2_start_idx
    end

    test "capture stdout" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "echo hello_stdout")
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "hello_stdout"))
    end

    test "capture stderr" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "echo hello_stderr >&2")
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stderr_of(events), &String.contains?(&1, "hello_stderr"))
    end

    test "non-zero command exit fails pipeline" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "exit 1")
        |> run_pipeline()

      assert {:error, _} = await_pipeline(pid)
    end

    test "non-zero command stops further execution" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "exit 1")
        |> Crank.step("step2", "echo should_not_run")
        |> run_pipeline()

      events = collect_events(pid)

      assert Enum.any?(events, &match?({:command_failed, ^pid, _}, &1))

      started_cmds =
        for {:command_started, ^pid, %{cmd: cmd}} <- events, do: cmd

      refute "echo should_not_run" in started_cmds
    end

    # erlexec wraps string commands in `sh -c`, so a missing binary exits 127
    # rather than failing in Command.Server.init. The init error path requires
    # passing a list to :exec.run, which the current string-only API does not support.
    test "non-existent command fails pipeline" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", "/doesnt-exist")
        |> run_pipeline()

      assert {:error, _} = await_pipeline(pid)
    end

    test "function step runs command via Crank.cmd!" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", fn _ctx, cmd_opts -> Crank.cmd!("echo from_fn", cmd_opts) end)
        |> run_pipeline()

      events = collect_events(pid)
      assert Enum.any?(stdout_of(events), &String.contains?(&1, "from_fn"))
    end

    test "function step with multiple Crank.cmd! calls runs them sequentially" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", fn _ctx, cmd_opts ->
          Crank.cmd!("echo cmd_one", cmd_opts)
          Crank.cmd!("echo cmd_two", cmd_opts)
        end)
        |> run_pipeline()

      events = collect_events(pid)
      cmd_events = command_events(events)

      assert [
               {:command_started, ^pid, %{cmd: "echo cmd_one"}},
               {:command_finished, ^pid, _},
               {:command_started, ^pid, %{cmd: "echo cmd_two"}},
               {:command_finished, ^pid, _}
             ] = cmd_events
    end

    test "function step that raises fails the pipeline" do
      {:ok, pid} =
        Crank.new()
        |> Crank.step("step1", fn _ctx, _cmd_opts -> raise "boom" end)
        |> run_pipeline()

      assert {:error, _} = await_pipeline(pid)
    end
  end
end
