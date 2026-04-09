defmodule CrankTest do
  use ExUnit.Case, async: true
  import Crank.Test.PipelineHelpers
  alias Crank.{Pipeline, Step}

  test "step" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", "echo hello"))
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
  end

  test "multiple commands in step" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", ["echo a", "echo b"]))
      |> run_pipeline()

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
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", "echo first"))
      |> Pipeline.add(Step.new("step2", "echo second"))
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
      |> Pipeline.add(Step.new("step1", "echo hello_stdout"))
      |> run_pipeline()

    events = collect_events(pid)

    stdout_data = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout_data, &String.contains?(&1, "hello_stdout"))
  end

  test "capture stderr" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", "echo hello_stderr >&2"))
      |> run_pipeline()

    events = collect_events(pid)

    stderr_data = for {:command_stderr, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stderr_data, &String.contains?(&1, "hello_stderr"))
  end

  test "non-zero command exit fails pipeline" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", "exit 1"))
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "non-zero command stops further execution" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", "exit 1"))
      |> Pipeline.add(Step.new("step2", "echo should_not_run"))
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
      |> Pipeline.add(Step.new("step1", "/doesnt-exist"))
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "function step runs command via Crank.cmd!" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(
        Step.new("step1", fn _ctx, cmd_opts -> Crank.cmd!("echo from_fn", cmd_opts) end)
      )
      |> run_pipeline()

    events = collect_events(pid)

    stdout_data = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout_data, &String.contains?(&1, "from_fn"))
  end

  test "function step with multiple Crank.cmd! calls runs them sequentially" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(
        Step.new("step1", fn _ctx, cmd_opts ->
          Crank.cmd!("echo cmd_one", cmd_opts)
          Crank.cmd!("echo cmd_two", cmd_opts)
        end)
      )
      |> run_pipeline()

    events = collect_events(pid)

    cmd_events =
      for {t, _, _} = event <- events, t in [:command_started, :command_finished], do: event

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
      |> Pipeline.add(Step.new("step1", fn _ctx, _cmd_opts -> raise "boom" end))
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "initial ctx is passed to the first step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{hello: :world})
      |> Pipeline.add(Step.new("step1", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end))
      |> run_pipeline()

    assert_receive {:ctx, %{hello: :world}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "ctx_add merges keys into ctx for the next step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", fn _ctx, _cmd_opts -> {:ctx_add, %{key: :value}} end))
      |> Pipeline.add(Step.new("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end))
      |> run_pipeline()

    assert_receive {:ctx, %{key: :value}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "ctx_add with conflicting keys fails the pipeline" do
    {:ok, pid} =
      Crank.new(%{key: :original})
      |> Pipeline.add(Step.new("step1", fn _ctx, _cmd_opts -> {:ctx_add, %{key: :new}} end))
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "ctx_set replaces ctx entirely for the next step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{old: :key})
      |> Pipeline.add(Step.new("step1", fn _ctx, _cmd_opts -> {:ctx_set, %{fresh: :ctx}} end))
      |> Pipeline.add(Step.new("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end))
      |> run_pipeline()

    assert_receive {:ctx, received_ctx}
    assert received_ctx == %{fresh: :ctx}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "ctx_set preserves raw key" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{raw: %{argv: []}, old: :key})
      |> Pipeline.add(Step.new("step1", fn _ctx, _cmd_opts -> {:ctx_set, %{fresh: :ctx}} end))
      |> Pipeline.add(Step.new("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end))
      |> run_pipeline()

    assert_receive {:ctx, received_ctx}
    assert received_ctx == %{fresh: :ctx, raw: %{argv: []}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "non-matching step return value leaves ctx unchanged" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", fn _ctx, _cmd_opts -> :some_other_value end))
      |> Pipeline.add(Step.new("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end))
      |> run_pipeline()

    assert_receive {:ctx, %{}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "isolated pipelines" do
    {:ok, pid1} =
      Crank.new()
      |> Pipeline.add(Step.new("step1", "echo pipeline1_output"))
      |> run_pipeline()

    {:ok, pid2} =
      Crank.new()
      |> Pipeline.add(Step.new("step2", "echo pipeline2_output"))
      |> run_pipeline()

    events1 = collect_events(pid1)
    events2 = collect_events(pid2)

    stdout1 = for {:command_stdout, ^pid1, %{data: d}} <- events1, do: d
    stdout2 = for {:command_stdout, ^pid2, %{data: d}} <- events2, do: d

    assert Enum.any?(stdout1, &String.contains?(&1, "pipeline1_output"))
    assert Enum.any?(stdout2, &String.contains?(&1, "pipeline2_output"))

    refute Enum.any?(stdout1, &String.contains?(&1, "pipeline2_output"))
    refute Enum.any?(stdout2, &String.contains?(&1, "pipeline1_output"))
  end

  test "group finishes when all steps finish" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", "echo a"), Step.new("s2", "echo b")])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_started, ^pid, %{name: "g"}}, &1))
    assert Enum.any?(events, &match?({:group_finished, ^pid, %{name: "g"}}, &1))
    assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
  end

  test "steps in group have group_id in event data" do
    pipeline =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", "echo a")])

    [group] = pipeline.items
    {:ok, pid} = run_pipeline(pipeline)
    events = collect_events(pid)

    step_events = for {:step_started, ^pid, data} <- events, do: data
    assert Enum.all?(step_events, &(&1.group_id == group.id))
  end

  test "group ctx_add values are merged into ctx for the next step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [
        Step.new("s1", fn _ctx, _opts -> {:ctx_add, %{a: 1}} end),
        Step.new("s2", fn _ctx, _opts -> {:ctx_add, %{b: 2}} end)
      ])
      |> Pipeline.add(Step.new("check", fn ctx, _opts -> send(test_pid, {:ctx, ctx}) end))
      |> run_pipeline()

    assert_receive {:ctx, ctx}
    assert ctx.a == 1
    assert ctx.b == 2
    assert {:ok, _} = await_pipeline(pid)
  end

  test "group fails if any step fails" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", "echo ok"), Step.new("s2", "exit 1")])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_failed, ^pid, %{name: "g"}}, &1))
    assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
  end

  test "group fails if a step returns ctx_set" do
    step = Step.new("s1", fn _ctx, _opts -> {:ctx_set, %{x: 1}} end)

    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [step])
      |> run_pipeline()

    events = collect_events(pid)
    step_id = step.id
    assert Enum.any?(events, &match?({:group_failed, ^pid, %{reason: {:ctx_set_not_allowed, ^step_id}}}, &1))
  end

  test "group fails if two steps return the same ctx_add key" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [
        Step.new("s1", fn _ctx, _opts -> {:ctx_add, %{key: 1}} end),
        Step.new("s2", fn _ctx, _opts -> {:ctx_add, %{key: 2}} end)
      ])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_failed, ^pid, _}, &1))
  end

  test "group ctx_add key conflicts with existing pipeline ctx fails the pipeline" do
    {:ok, pid} =
      Crank.new(%{key: :original})
      |> Crank.group("g", [Step.new("s1", fn _ctx, _opts -> {:ctx_add, %{key: :new}} end)])
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "pipeline cd" do
    {:ok, pid} =
      Crank.new(%{}, cd: "/tmp")
      |> Pipeline.add(Step.new("s1", "pwd"))
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "tmp"))
  end

  test "step cd overrides pipeline cd" do
    {:ok, pid} =
      Crank.new(%{}, cd: "/tmp")
      |> Pipeline.add(Step.new("s1", "pwd", cd: "/var"))
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "var"))
    refute Enum.any?(stdout, fn d -> String.trim(d) == "/tmp" end)
  end

  test "group cd inherited by steps" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", "pwd")], cd: "/tmp")
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "tmp"))
  end

  test "step cd overrides group cd" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", "pwd", cd: "/var")], cd: "/tmp")
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "var"))
    refute Enum.any?(stdout, fn d -> String.trim(d) == "/tmp" end)
  end

  test "step timeout" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("s1", fn _ctx, _opts -> Process.sleep(5_000) end, timeout: 50))
      |> run_pipeline()

    events = collect_events(pid, 500)
    assert Enum.any?(events, &match?({:step_failed, ^pid, %{reason: :timeout}}, &1))
    assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
  end

  test "step within timeout succeeds" do
    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("s1", "echo ok", timeout: 5_000))
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
  end

  test "group timeout" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", fn _ctx, _opts -> Process.sleep(5_000) end)], timeout: 50)
      |> run_pipeline()

    events = collect_events(pid, 500)
    assert Enum.any?(events, &match?({:group_failed, ^pid, %{reason: :timeout}}, &1))
    assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
  end

  test "step if: boolean" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Pipeline.add(Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: false))
      |> Pipeline.add(Step.new("s2", fn _ctx, _opts -> send(test_pid, :done) end))
      |> run_pipeline()

    events = collect_events(pid)
    assert_receive :done
    refute_received :ran
    assert Enum.any?(events, &match?({:step_skipped, ^pid, %{name: "s1"}}, &1))
    assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))

    {:ok, pid2} =
      Crank.new()
      |> Pipeline.add(Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: true))
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid2)
    assert_receive :ran
  end

  test "step if: atom" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{run_it: true})
      |> Pipeline.add(Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: :run_it))
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
    assert_receive :ran

    {:ok, pid2} =
      Crank.new(%{run_it: false})
      |> Pipeline.add(Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: :run_it))
      |> Pipeline.add(Step.new("s2", fn _ctx, _opts -> send(test_pid, :done) end))
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid2)
    assert_receive :done
    refute_received :ran
  end

  test "step if: fn" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{x: 5})
      |> Pipeline.add(Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: fn ctx -> ctx.x > 3 end))
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
    assert_receive :ran
  end

  test "group if: false skips" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end)], if: false)
      |> Pipeline.add(Step.new("s2", fn _ctx, _opts -> send(test_pid, :done) end))
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
      Crank.new()
      |> Crank.group("g", [
        Step.new("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: false),
        Step.new("s2", fn _ctx, _opts -> send(test_pid, :other) end)
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
      Crank.new()
      |> Crank.group("g", [Step.new("s1", "echo hi", if: false)])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_skipped, ^pid, %{name: "g"}}, &1))
    refute Enum.any?(events, &match?({:group_finished, ^pid, _}, &1))
    assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
  end
end
