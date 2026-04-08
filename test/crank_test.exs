defmodule CrankTest do
  use ExUnit.Case, async: true
  import Crank.Test.PipelineHelpers

  test "step" do
    {:ok, pid} = run_pipeline([{"step1", "echo hello"}])
    assert {:ok, _} = await_pipeline(pid)
  end

  test "multiple commands in step" do
    {:ok, pid} = run_pipeline([{"step1", ["echo a", "echo b"]}])
    events = collect_events(pid)

    cmd_events =
      for {t, _, _} = event <- events,
          t in [:command_started, :command_finished] do
        event
      end

    assert [
             {:command_started, ^pid, %{cmd: "echo a"}},
             {:command_finished, ^pid, _},
             {:command_started, ^pid, %{cmd: "echo b"}},
             {:command_finished, ^pid, _}
           ] = cmd_events
  end

  test "multiple sequential steps" do
    {:ok, pid} = run_pipeline([{"step1", "echo first"}, {"step2", "echo second"}])
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
    {:ok, pid} = run_pipeline([{"step1", "echo hello_stdout"}])
    events = collect_events(pid)

    stdout_data = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout_data, &String.contains?(&1, "hello_stdout"))
  end

  test "capture stderr" do
    {:ok, pid} = run_pipeline([{"step1", "echo hello_stderr >&2"}])
    events = collect_events(pid)

    stderr_data = for {:command_stderr, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stderr_data, &String.contains?(&1, "hello_stderr"))
  end

  test "non-zero command exit fails pipeline" do
    {:ok, pid} = run_pipeline([{"step1", "exit 1"}])
    assert {:error, _} = await_pipeline(pid)
  end

  test "non-zero command stops further execution" do
    {:ok, pid} =
      run_pipeline([
        {"step1", "exit 1"},
        {"step2", "echo should_not_run"}
      ])

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
    {:ok, pid} = run_pipeline([{"step1", "/doesnt-exist"}])
    assert {:error, _} = await_pipeline(pid)
  end

  test "isolated pipelines" do
    {:ok, pid1} = run_pipeline([{"step1", "echo pipeline1_output"}])
    {:ok, pid2} = run_pipeline([{"step2", "echo pipeline2_output"}])

    events1 = collect_events(pid1)
    events2 = collect_events(pid2)

    stdout1 = for {:command_stdout, ^pid1, %{data: d}} <- events1, do: d
    stdout2 = for {:command_stdout, ^pid2, %{data: d}} <- events2, do: d

    assert Enum.any?(stdout1, &String.contains?(&1, "pipeline1_output"))
    assert Enum.any?(stdout2, &String.contains?(&1, "pipeline2_output"))

    refute Enum.any?(stdout1, &String.contains?(&1, "pipeline2_output"))
    refute Enum.any?(stdout2, &String.contains?(&1, "pipeline1_output"))
  end
end
