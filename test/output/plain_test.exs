defmodule Crank.Output.PlainTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias Crank.Output.Plain
  alias Crank.Output.Utils

  describe "format_duration/1" do
    test "milliseconds under 1s" do
      assert Utils.format_duration(0) == "0ms"
      assert Utils.format_duration(30) == "30ms"
      assert Utils.format_duration(999) == "999ms"
    end

    test "seconds under 1m" do
      assert Utils.format_duration(1000) == "1s"
      assert Utils.format_duration(200) == "200ms"
      assert Utils.format_duration(1200) == "1.2s"
      assert Utils.format_duration(530) == "530ms"
      assert Utils.format_duration(1530) == "1.53s"
      assert Utils.format_duration(59_990) == "59.99s"
    end

    test "minutes under 1h" do
      assert Utils.format_duration(60_000) == "1m0s"
      assert Utils.format_duration(106_000) == "1m46s"
      assert Utils.format_duration(3_599_000) == "59m59s"
    end

    test "hours" do
      assert Utils.format_duration(3_600_000) == "1h0m"
      assert Utils.format_duration(4_320_000) == "1h12m"
    end
  end

  defp make_ref_ids do
    %{pipeline: make_ref(), step: make_ref(), cmd: make_ref(), group: make_ref()}
  end

  defp run_events(events) do
    capture_io(fn ->
      state = Plain.init([])
      Enum.reduce(events, state, &Plain.handle_event/2)
    end)
  end

  test "success: start header, step lines, summary" do
    ids = make_ref_ids()
    t0 = 1000
    t1 = t0 + 230

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:step_started, ids.pipeline,
         %{id: ids.step, name: "compile", group_id: nil, now_ms: t0}},
        {:command_started, ids.pipeline,
         %{id: ids.cmd, cmd: "mix compile", step_id: ids.step, now_ms: t0}},
        {:command_stdout, ids.pipeline,
         %{id: ids.cmd, step_id: ids.step, data: "Compiled ok\n", now_ms: t1}},
        {:command_finished, ids.pipeline, %{id: ids.cmd, step_id: ids.step, now_ms: t1}},
        {:step_finished, ids.pipeline,
         %{id: ids.step, name: "compile", group_id: nil, now_ms: t1}},
        {:pipeline_finished, ids.pipeline, %{now_ms: t1}}
      ])

    assert output =~ ~r/^Start \(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\)/m
    assert output =~ ">> compile\n"
    assert output =~ "ok compile  230ms\n"
    assert output =~ ~r/OK: 1 ok, 0 skipped \| started \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z \| 230ms total/
    refute output =~ "Compiled ok"
  end

  test "failure: error line with exit code and detail block" do
    ids = make_ref_ids()
    t0 = 1000
    t1 = t0 + 500

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:step_started, ids.pipeline,
         %{id: ids.step, name: "compile", group_id: nil, now_ms: t0}},
        {:command_started, ids.pipeline,
         %{id: ids.cmd, cmd: "mix compile", step_id: ids.step, now_ms: t0}},
        {:command_stderr, ids.pipeline,
         %{id: ids.cmd, step_id: ids.step, data: "error on line 5\n", now_ms: t1}},
        {:command_failed, ids.pipeline,
         %{id: ids.cmd, step_id: ids.step, exit_code: 1, now_ms: t1}},
        {:step_failed, ids.pipeline,
         %{
           id: ids.step,
           name: "compile",
           group_id: nil,
           reason: {:action_error, %Crank.CommandError{exit_code: 1, cmd: "mix compile"}},
           now_ms: t1
         }},
        {:pipeline_failed, ids.pipeline,
         %{
           reason: {:action_error, %Crank.CommandError{exit_code: 1, cmd: "mix compile"}},
           now_ms: t1
         }}
      ])

    assert output =~ "FAIL compile  500ms  (exit code: 1)\n"
    assert output =~ "  $ mix compile  [exit 1]\n"
    assert output =~ "    error on line 5\n"
    refute output =~ "stdout:"
    assert output =~ ~r/FAIL: failed compile \| 0 ok, 0 skipped \| started \S+ \| 500ms total/
  end

  test "failure: timeout reason" do
    ids = make_ref_ids()
    t0 = 1000
    t1 = t0 + 30_000

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:step_started, ids.pipeline, %{id: ids.step, name: "slow", group_id: nil, now_ms: t0}},
        {:step_failed, ids.pipeline,
         %{id: ids.step, name: "slow", group_id: nil, reason: :timeout, now_ms: t1}},
        {:pipeline_failed, ids.pipeline, %{reason: :timeout, now_ms: t1}}
      ])

    assert output =~ "FAIL slow  30s  (timeout)\n"
    assert output =~ ~r/FAIL: failed slow \| 0 ok, 0 skipped \| started \S+ \| 30s total/
  end

  test "failure: exception in action fn" do
    ids = make_ref_ids()
    t0 = 1000
    t1 = t0 + 100

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:step_started, ids.pipeline, %{id: ids.step, name: "boom", group_id: nil, now_ms: t0}},
        {:step_failed, ids.pipeline,
         %{
           id: ids.step,
           name: "boom",
           group_id: nil,
           reason: {:action_error, %RuntimeError{message: "oops"}},
           now_ms: t1
         }},
        {:pipeline_failed, ids.pipeline,
         %{reason: {:action_error, %RuntimeError{message: "oops"}}, now_ms: t1}}
      ])

    assert output =~ "FAIL boom  100ms  (exception)\n"
  end

  test "skipped step: shown in output and summary count" do
    ids = make_ref_ids()
    t0 = 1000
    t1 = t0 + 50

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:step_skipped, ids.pipeline, %{id: make_ref(), name: "lint", group_id: nil, now_ms: t0}},
        {:step_started, ids.pipeline, %{id: ids.step, name: "test", group_id: nil, now_ms: t0}},
        {:step_finished, ids.pipeline, %{id: ids.step, name: "test", group_id: nil, now_ms: t1}},
        {:pipeline_finished, ids.pipeline, %{now_ms: t1}}
      ])

    assert output =~ "skip lint\n"
    assert output =~ ~r/OK: 1 ok, 1 skipped \| started \S+ \| 50ms total/
  end

  test "group: header with step count, indented steps, ok summary" do
    ids = make_ref_ids()
    s1 = make_ref()
    s2 = make_ref()
    t0 = 1000
    t1 = t0 + 2000

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:group_started, ids.pipeline,
         %{id: ids.group, name: "checks", step_count: 2, now_ms: t0}},
        {:step_started, ids.pipeline, %{id: s1, name: "credo", group_id: ids.group, now_ms: t0}},
        {:step_started, ids.pipeline,
         %{id: s2, name: "dialyzer", group_id: ids.group, now_ms: t0}},
        {:step_finished, ids.pipeline,
         %{id: s1, name: "credo", group_id: ids.group, now_ms: t0 + 1000}},
        {:step_finished, ids.pipeline,
         %{id: s2, name: "dialyzer", group_id: ids.group, now_ms: t1}},
        {:group_finished, ids.pipeline, %{id: ids.group, name: "checks", now_ms: t1}},
        {:pipeline_finished, ids.pipeline, %{now_ms: t1}}
      ])

    assert output =~ ">> checks (2 steps)\n"
    assert output =~ "  >> credo\n"
    assert output =~ "  >> dialyzer\n"
    assert output =~ "  ok credo  1s\n"
    assert output =~ "  ok dialyzer  2s\n"
    assert output =~ "ok checks (2/2)  2s\n"
    assert output =~ ~r/OK: 2 ok, 0 skipped \| started \S+ \| 2s total/
  end

  test "group failure: detail block indented, group error line, summary" do
    ids = make_ref_ids()
    s1 = make_ref()
    s2 = make_ref()
    c2 = make_ref()
    t0 = 1000
    t1 = t0 + 1000
    t2 = t0 + 5000

    output =
      run_events([
        {:pipeline_started, ids.pipeline, %{now_ms: t0}},
        {:group_started, ids.pipeline,
         %{id: ids.group, name: "checks", step_count: 2, now_ms: t0}},
        {:step_started, ids.pipeline, %{id: s1, name: "credo", group_id: ids.group, now_ms: t0}},
        {:step_started, ids.pipeline,
         %{id: s2, name: "dialyzer", group_id: ids.group, now_ms: t0}},
        {:command_started, ids.pipeline, %{id: c2, cmd: "mix dialyzer", step_id: s2, now_ms: t0}},
        {:step_finished, ids.pipeline, %{id: s1, name: "credo", group_id: ids.group, now_ms: t1}},
        {:command_stderr, ids.pipeline, %{id: c2, step_id: s2, data: "Type error\n", now_ms: t2}},
        {:command_failed, ids.pipeline, %{id: c2, step_id: s2, exit_code: 1, now_ms: t2}},
        {:step_failed, ids.pipeline,
         %{
           id: s2,
           name: "dialyzer",
           group_id: ids.group,
           reason: {:action_error, %Crank.CommandError{exit_code: 1, cmd: "mix dialyzer"}},
           now_ms: t2
         }},
        {:group_failed, ids.pipeline,
         %{id: ids.group, name: "checks", reason: :propagated, now_ms: t2}},
        {:pipeline_failed, ids.pipeline, %{reason: :propagated, now_ms: t2}}
      ])

    assert output =~ "  ok credo  1s\n"
    assert output =~ "  FAIL dialyzer  5s  (exit code: 1)\n"
    assert output =~ "    $ mix dialyzer  [exit 1]\n"
    assert output =~ "      Type error\n"
    assert output =~ "FAIL checks (2/2)  5s\n"
    assert output =~ ~r/FAIL: failed dialyzer \| 1 ok, 0 skipped \| started \S+ \| 5s total/
  end
end
