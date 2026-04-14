defmodule CrankTest do
  use ExUnit.Case, async: true
  import Crank.Test.PipelineHelpers

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

    stdout_data = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout_data, &String.contains?(&1, "hello_stdout"))
  end

  test "capture stderr" do
    {:ok, pid} =
      Crank.new()
      |> Crank.step("step1", "echo hello_stderr >&2")
      |> run_pipeline()

    events = collect_events(pid)

    stderr_data = for {:command_stderr, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stderr_data, &String.contains?(&1, "hello_stderr"))
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

    stdout_data = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout_data, &String.contains?(&1, "from_fn"))
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
      |> Crank.step("step1", fn _ctx, _cmd_opts -> raise "boom" end)
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "initial ctx is passed to the first step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{hello: :world})
      |> Crank.step("step1", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
      |> run_pipeline()

    assert_receive {:ctx, %{hello: :world}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "ctx_add merges keys into ctx for the next step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.step("step1", fn _ctx, _cmd_opts -> {:ctx_add, %{key: :value}} end)
      |> Crank.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
      |> run_pipeline()

    assert_receive {:ctx, %{key: :value}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "ctx_add with conflicting keys fails the pipeline" do
    {:ok, pid} =
      Crank.new(%{key: :original})
      |> Crank.step("step1", fn _ctx, _cmd_opts -> {:ctx_add, %{key: :new}} end)
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "ctx_set replaces ctx entirely for the next step" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{old: :key})
      |> Crank.step("step1", fn _ctx, _cmd_opts -> {:ctx_set, %{fresh: :ctx}} end)
      |> Crank.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
      |> run_pipeline()

    assert_receive {:ctx, received_ctx}
    assert received_ctx == %{fresh: :ctx}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "ctx_set preserves raw key" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{raw: %{argv: []}, old: :key})
      |> Crank.step("step1", fn _ctx, _cmd_opts -> {:ctx_set, %{fresh: :ctx}} end)
      |> Crank.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
      |> run_pipeline()

    assert_receive {:ctx, received_ctx}
    assert received_ctx == %{fresh: :ctx, raw: %{argv: []}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "non-matching step return value leaves ctx unchanged" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.step("step1", fn _ctx, _cmd_opts -> :some_other_value end)
      |> Crank.step("step2", fn ctx, _cmd_opts -> send(test_pid, {:ctx, ctx}) end)
      |> run_pipeline()

    assert_receive {:ctx, %{}}
    assert {:ok, _} = await_pipeline(pid)
  end

  test "isolated pipelines" do
    {:ok, pid1} =
      Crank.new()
      |> Crank.step("step1", "echo pipeline1_output")
      |> run_pipeline()

    {:ok, pid2} =
      Crank.new()
      |> Crank.step("step2", "echo pipeline2_output")
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
      |> Crank.group("g", [Crank.step("s1", "echo a"), Crank.step("s2", "echo b")])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_started, ^pid, %{name: "g"}}, &1))
    assert Enum.any?(events, &match?({:group_finished, ^pid, %{name: "g"}}, &1))
    assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
  end

  test "steps in group have group_id in event data" do
    group =
      Crank.group("g")
      |> Crank.step("s1", "echo a")

    pipeline =
      Crank.new()
      |> Crank.group(group)

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
        Crank.step("s1", fn _ctx, _opts -> {:ctx_add, %{a: 1}} end),
        Crank.step("s2", fn _ctx, _opts -> {:ctx_add, %{b: 2}} end)
      ])
      |> Crank.step("check", fn ctx, _opts -> send(test_pid, {:ctx, ctx}) end)
      |> run_pipeline()

    assert_receive {:ctx, ctx}
    assert ctx.a == 1
    assert ctx.b == 2
    assert {:ok, _} = await_pipeline(pid)
  end

  test "group fails if any step fails" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Crank.step("s1", "echo ok"), Crank.step("s2", "exit 1")])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_failed, ^pid, %{name: "g"}}, &1))
    assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
  end

  test "group fails if a step returns ctx_set" do
    step = Crank.step("s1", fn _ctx, _opts -> {:ctx_set, %{x: 1}} end)

    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [step])
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
      Crank.new()
      |> Crank.group("g", [
        Crank.step("s1", fn _ctx, _opts -> {:ctx_add, %{key: 1}} end),
        Crank.step("s2", fn _ctx, _opts -> {:ctx_add, %{key: 2}} end)
      ])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_failed, ^pid, _}, &1))
  end

  test "group ctx_add key conflicts with existing pipeline ctx fails the pipeline" do
    {:ok, pid} =
      Crank.new(%{key: :original})
      |> Crank.group("g", [Crank.step("s1", fn _ctx, _opts -> {:ctx_add, %{key: :new}} end)])
      |> run_pipeline()

    assert {:error, _} = await_pipeline(pid)
  end

  test "pipeline cd" do
    {:ok, pid} =
      Crank.new(%{}, cd: "/tmp")
      |> Crank.step("s1", "pwd")
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "tmp"))
  end

  test "step cd overrides pipeline cd" do
    {:ok, pid} =
      Crank.new(%{}, cd: "/tmp")
      |> Crank.step("s1", "pwd", cd: "/var")
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "var"))
    refute Enum.any?(stdout, fn d -> String.trim(d) == "/tmp" end)
  end

  test "group cd inherited by steps" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Crank.step("s1", "pwd")], cd: "/tmp")
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "tmp"))
  end

  test "step cd overrides group cd" do
    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Crank.step("s1", "pwd", cd: "/var")], cd: "/tmp")
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "var"))
    refute Enum.any?(stdout, fn d -> String.trim(d) == "/tmp" end)
  end

  test "step timeout" do
    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _opts -> Process.sleep(5_000) end, timeout: 50)
      |> run_pipeline()

    events = collect_events(pid, 500)
    assert Enum.any?(events, &match?({:step_failed, ^pid, %{reason: :timeout}}, &1))
    assert Enum.any?(events, &match?({:pipeline_failed, ^pid, _}, &1))
  end

  test "step within timeout succeeds" do
    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", "echo ok", timeout: 5_000)
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
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

  test "step if: boolean" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: false)
      |> Crank.step("s2", fn _ctx, _opts -> send(test_pid, :done) end)
      |> run_pipeline()

    events = collect_events(pid)
    assert_receive :done
    refute_received :ran
    assert Enum.any?(events, &match?({:step_skipped, ^pid, %{name: "s1"}}, &1))
    assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))

    {:ok, pid2} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: true)
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid2)
    assert_receive :ran
  end

  test "step if: atom" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{run_it: true})
      |> Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: :run_it)
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
    assert_receive :ran

    {:ok, pid2} =
      Crank.new(%{run_it: false})
      |> Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: :run_it)
      |> Crank.step("s2", fn _ctx, _opts -> send(test_pid, :done) end)
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid2)
    assert_receive :done
    refute_received :ran
  end

  test "step if: fn" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{x: 5})
      |> Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: fn ctx -> ctx.x > 3 end)
      |> run_pipeline()

    assert {:ok, _} = await_pipeline(pid)
    assert_receive :ran
  end

  test "group if: false skips" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.group("g", [Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end)],
        if: false
      )
      |> Crank.step("s2", fn _ctx, _opts -> send(test_pid, :done) end)
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
        Crank.step("s1", fn _ctx, _opts -> send(test_pid, :ran) end, if: false),
        Crank.step("s2", fn _ctx, _opts -> send(test_pid, :other) end)
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
      |> Crank.group("g", [Crank.step("s1", "echo hi", if: false)])
      |> run_pipeline()

    events = collect_events(pid)
    assert Enum.any?(events, &match?({:group_skipped, ^pid, %{name: "g"}}, &1))
    refute Enum.any?(events, &match?({:group_finished, ^pid, _}, &1))
    assert Enum.any?(events, &match?({:pipeline_finished, ^pid, _}, &1))
  end

  test "await/2 returns the final ctx on success" do
    {:ok, pipeline_id} =
      Crank.new(%{start: :ok})
      |> Crank.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
      |> Crank.run()

    assert {:ok, %{start: :ok, done: true}} = Crank.await(pipeline_id)
  end

  test "await/2 returns the failure reason and last ctx" do
    {:ok, pipeline_id} =
      Crank.new(%{start: :ok})
      |> Crank.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
      |> Crank.step("boom", fn _ctx, _opts -> raise "boom" end)
      |> Crank.run()

    assert {:error, {:action_error, %RuntimeError{message: "boom"}}, %{start: :ok, done: true}} =
             Crank.await(pipeline_id)
  end

  test "await/2 works for late awaiters in the owner process and consumes the result" do
    {:ok, pipeline_id} =
      Crank.new()
      |> Crank.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
      |> run_pipeline()

    assert {:ok, _data} = await_pipeline(pipeline_id)
    assert {:ok, %{done: true}} = Crank.await(pipeline_id)
    assert {:error, :timeout} = Crank.await(pipeline_id, 0)
  end

  test "await/2 with a list returns results in input order" do
    test_pid = self()

    slow_pipeline =
      Crank.new()
      |> Crank.step("slow", fn _ctx, _opts ->
        send(test_pid, {:slow_step, self()})

        receive do
          :release -> {:ctx_add, %{name: :slow}}
        end
      end)

    fast_pipeline =
      Crank.new()
      |> Crank.step("fast", fn _ctx, _opts -> {:ctx_add, %{name: :fast}} end)

    fast_id = fast_pipeline.id
    Crank.Output.Test.subscribe(fast_id, self())
    ExUnit.Callbacks.on_exit(fn -> Crank.Output.Test.unsubscribe(fast_id) end)

    {:ok, slow_id} = Crank.run(slow_pipeline)
    {:ok, ^fast_id} = Crank.run(fast_pipeline)

    assert_receive {:slow_step, slow_step_pid}
    assert {:ok, _data} = await_pipeline(fast_id)
    send(slow_step_pid, :release)

    assert [
             {:ok, %{name: :fast}},
             {:ok, %{name: :slow}}
           ] = Crank.await([fast_id, slow_id], 1_000)
  end

  test "await/2 timeout does not consume the later result" do
    test_pid = self()

    {:ok, pipeline_id} =
      Crank.new()
      |> Crank.step("wait", fn _ctx, _opts ->
        send(test_pid, {:step_pid, self()})

        receive do
          :release -> {:ctx_add, %{done: true}}
        end
      end)
      |> Crank.run()

    assert_receive {:step_pid, step_pid}
    assert {:error, :timeout} = Crank.await(pipeline_id, 0)
    send(step_pid, :release)
    assert {:ok, %{done: true}} = Crank.await(pipeline_id)
  end

  test "stop/2 gracefully stops an active pipeline" do
    test_pid = self()

    {:ok, pipeline_id} =
      Crank.new()
      |> Crank.step("wait", fn _ctx, _opts ->
        send(test_pid, {:step_pid, self()})

        receive do
          :release -> :ok
        end
      end)
      |> Crank.run()

    assert_receive {:step_pid, step_pid}
    assert :ok = Crank.stop(pipeline_id, :graceful)
    assert {:error, {:stopped, :graceful}, %{}} = Crank.await(pipeline_id)
    assert_process_down(step_pid)
  end

  test "await/2 is owner-scoped" do
    test_pid = self()

    owner =
      spawn(fn ->
        pipeline =
          Crank.new()
          |> Crank.step("wait", fn _ctx, _opts ->
            send(test_pid, {:step_pid, self()})

            receive do
              :release -> {:ctx_add, %{done: true}}
            end
          end)

        {:ok, pipeline_id} = Crank.run(pipeline)
        send(test_pid, {:pipeline_id, pipeline_id})

        receive do
          :await ->
            send(test_pid, {:owner_result, Crank.await(pipeline_id)})
        end
      end)

    assert is_pid(owner)
    assert_receive {:pipeline_id, pipeline_id}
    assert_receive {:step_pid, step_pid}
    assert {:error, :timeout} = Crank.await(pipeline_id, 0)
    send(step_pid, :release)
    send(owner, :await)
    assert_receive {:owner_result, {:ok, %{done: true}}}
  end

  test "owner exit stops the pipeline" do
    test_pid = self()

    pipeline =
      Crank.new()
      |> Crank.step("wait", fn _ctx, _opts ->
        send(test_pid, {:step_pid, self()})

        receive do
          :release -> :ok
        end
      end)

    pipeline_id = pipeline.id
    Crank.Output.Test.subscribe(pipeline_id, self())
    ExUnit.Callbacks.on_exit(fn -> Crank.Output.Test.unsubscribe(pipeline_id) end)

    owner =
      spawn(fn ->
        send(test_pid, {:run_result, Crank.Pipeline.start_pipeline(pipeline, owner: self())})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:run_result, {:ok, ^pipeline_id}}
    assert_receive {:step_pid, step_pid}

    pipeline_pid = Crank.Registry.lookup_pipeline(pipeline_id)
    assert is_pid(pipeline_pid)

    send(owner, :stop)

    assert_receive {:crank_event,
                    {:pipeline_failed, ^pipeline_id,
                     %{reason: {:stopped, {:owner_down, :normal}}}}}

    assert_process_down(step_pid)
    assert_process_down(pipeline_pid)
  end

  test "cd: atom reads from ctx" do
    {:ok, pid} =
      Crank.new(%{work: "/tmp"})
      |> Crank.step("s1", "pwd", cd: :work)
      |> run_pipeline()

    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, "tmp"))
  end

  test "cd: fn receives ctx and run_opts" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{base: "/tmp"})
      |> Crank.step("s1", fn _ctx, _opts -> :ok end,
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

  test "tmp_dir! creates a directory that survives pipeline finish" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!(:transient)
        send(test_pid, {:dir, dir})
      end)
      |> run_pipeline()

    assert_receive {:dir, dir}
    assert File.dir?(dir)
    assert {:ok, _} = await_pipeline(pid)
    assert File.dir?(dir)

    File.rm_rf!(dir)
  end

  test "tmp_dir! survives pipeline failure" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!(:transient)
        send(test_pid, {:dir, dir})
        raise "boom"
      end)
      |> run_pipeline()

    assert_receive {:dir, dir}
    assert {:error, _} = await_pipeline(pid)
    assert File.dir?(dir)

    File.rm_rf!(dir)
  end

  test "multiple tmp_dir! calls create distinct dirs" do
    test_pid = self()

    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _run_opts ->
        dir1 = Crank.tmp_dir!(:transient)
        dir2 = Crank.tmp_dir!(:transient)
        send(test_pid, {:dirs, dir1, dir2})
      end)
      |> run_pipeline()

    assert_receive {:dirs, dir1, dir2}
    assert dir1 != dir2
    assert {:ok, _} = await_pipeline(pid)
    assert File.dir?(dir1)
    assert File.dir?(dir2)

    File.rm_rf!(dir1)
    File.rm_rf!(dir2)
  end

  test "pipeline cd: fn with tmp_dir! uses it as working directory" do
    test_pid = self()

    {:ok, pid} =
      Crank.new(%{},
        cd: fn _ctx, _run_opts ->
          dir = Crank.tmp_dir!(:transient)
          send(test_pid, {:dir, dir})
          dir
        end
      )
      |> Crank.step("s1", "pwd")
      |> run_pipeline()

    assert_receive {:dir, dir}
    events = collect_events(pid)
    stdout = for {:command_stdout, ^pid, %{data: d}} <- events, do: d
    assert Enum.any?(stdout, &String.contains?(&1, Path.basename(dir)))
    assert File.dir?(dir)

    File.rm_rf!(dir)
  end

  test "tmp_dir! with key returns same path across pipelines" do
    test_pid = self()
    key = {:test_tmp_dir_reuse, make_ref()}
    write_data = fn dir, data -> File.write!(Path.join(dir, "datafile"), data) end
    read_data = fn dir -> File.read!(Path.join(dir, "datafile")) end

    {:ok, pid1} =
      Crank.new()
      |> Crank.step("writer", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!({:persistent, key: key, ttl: :infinity})
        write_data.(dir, "hello")
        send(test_pid, {:dir1, dir})
      end)
      |> run_pipeline()

    assert_receive {:dir1, tmp_dir}
    assert {:ok, _} = await_pipeline(pid1)

    {:ok, pid2} =
      Crank.new()
      |> Crank.step("reader", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!({:persistent, key: key, ttl: :infinity})
        send(test_pid, {:dir2, dir, read_data.(dir)})
      end)
      |> run_pipeline()

    assert_receive {:dir2, ^tmp_dir, "hello"}
    assert {:ok, _} = await_pipeline(pid2)

    File.rm_rf!(tmp_dir)
  end

  test "tmp_dir! with key can be reused by a later step in the same pipeline" do
    test_pid = self()
    key = {:test_tmp_dir_same_pipeline, make_ref()}
    write_data = fn dir, data -> File.write!(Path.join(dir, "datafile"), data) end
    read_data = fn dir -> File.read!(Path.join(dir, "datafile")) end

    {:ok, pid} =
      Crank.new()
      |> Crank.step("writer", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!({:persistent, key: key, ttl: :infinity})
        write_data.(dir, "hello")
        send(test_pid, {:writer, dir})
      end)
      |> Crank.step("reader", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!({:persistent, key: key, ttl: :infinity})
        send(test_pid, {:reader, dir, read_data.(dir)})
      end)
      |> run_pipeline()

    assert_receive {:writer, tmp_dir}
    assert_receive {:reader, ^tmp_dir, "hello"}
    assert {:ok, _} = await_pipeline(pid)

    File.rm_rf!(tmp_dir)
  end

  test "different tmp_dir! keys map to different paths" do
    test_pid = self()
    ref = make_ref()
    key1 = {:test_diff1, ref}
    key2 = {:test_diff2, ref}

    {:ok, pid} =
      Crank.new()
      |> Crank.step("s1", fn _ctx, _run_opts ->
        dir1 = Crank.tmp_dir!({:persistent, key: key1, ttl: :infinity})
        dir2 = Crank.tmp_dir!({:persistent, key: key2, ttl: :infinity})
        send(test_pid, {:dirs, dir1, dir2})
      end)
      |> run_pipeline()

    assert_receive {:dirs, tmp_dir1, tmp_dir2}
    assert tmp_dir1 != tmp_dir2
    assert {:ok, _} = await_pipeline(pid)

    File.rm_rf!(tmp_dir1)
    File.rm_rf!(tmp_dir2)
  end

  test "sweep does not delete a live script root while the creator pid is alive" do
    test_pid = self()

    write_data = fn dir, data ->
      File.write!(Path.join(dir, "datafile"), data)
      Path.join(dir, "datafile")
    end

    {:ok, pid} =
      Crank.new()
      |> Crank.step("holder", fn _ctx, _run_opts ->
        dir = Crank.tmp_dir!(:transient)
        filepath = write_data.(dir, "hello")
        send(test_pid, {:held_dir, self(), dir, filepath})

        receive do
          :release -> :ok
        after
          5_000 -> raise "timed out waiting for release"
        end
      end)
      |> run_pipeline()

    assert_receive {:held_dir, holder_pid, dir, filepath}

    Crank.TmpDir.sweep()

    assert File.dir?(dir)
    assert File.exists?(filepath)

    send(holder_pid, :release)
    assert {:ok, _} = await_pipeline(pid)
    assert File.dir?(dir)

    File.rm_rf!(dir)
  end

  test "sweep removes transient dirs from dead processes" do
    stale_dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-deadbeef")
    stale_file = Path.join(stale_dir, "marker")

    meta = %{
      pid: 999_999_999,
      key_hash: "deadbeef",
      created_at: 0,
      expires_at: 0
    }

    File.mkdir_p!(stale_dir)
    File.write!(stale_file, "hello")
    File.write!(Crank.TmpDir.metadata_path(stale_dir), JSON.encode!(meta))

    Crank.TmpDir.sweep()

    refute File.exists?(stale_dir)
    refute File.exists?(stale_file)
  end

  test "sweep removes dirs without metadata" do
    orphan_dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-orphan")
    orphan_file = Path.join(orphan_dir, "marker")

    File.mkdir_p!(orphan_dir)
    File.write!(orphan_file, "hello")

    Crank.TmpDir.sweep()

    refute File.exists?(orphan_dir)
    refute File.exists?(orphan_file)
  end

  test "sweep does not remove an expired persistent dir while the owner pid is alive" do
    dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-live-persistent")
    marker = Path.join(dir, "marker")

    meta = %{
      pid: String.to_integer(System.pid()),
      key_hash: "live-persistent",
      created_at: 0,
      expires_at: 0
    }

    File.mkdir_p!(dir)
    File.write!(marker, "hello")
    File.write!(Crank.TmpDir.metadata_path(dir), JSON.encode!(meta))

    Crank.TmpDir.sweep()

    assert File.exists?(dir)
    assert File.exists?(marker)

    File.rm_rf!(dir)
  end

  test "sweep removes an expired persistent dir when the owner pid is dead" do
    dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "key-dead-persistent")
    marker = Path.join(dir, "marker")

    meta = %{
      pid: 999_999_999,
      key_hash: "dead-persistent",
      created_at: 0,
      expires_at: 0
    }

    File.mkdir_p!(dir)
    File.write!(marker, "hello")
    File.write!(Crank.TmpDir.metadata_path(dir), JSON.encode!(meta))

    Crank.TmpDir.sweep()

    refute File.exists?(dir)
    refute File.exists?(marker)
  end

  defp assert_process_down(pid, timeout \\ 500) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, timeout
  end
end
