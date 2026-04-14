defmodule Crank.SubscriptionsTest do
  use ExUnit.Case, async: false

  alias Crank.Subscriptions.PathSpec

  test "notify/1 delivers plain signals to subscribers" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe("plain notify", fn ctx, {:build_finished, %{exit_code: exit_code}} ->
        send(test_pid, {:signal, ctx, exit_code})
        :ok
      end)

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:build_finished, %{exit_code: 0}})
    assert_receive {:signal, %{}, 0}
  end

  test "subscribe/3 preserves subscriber ctx across invocations" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "ctx persistence",
        fn ctx, {:tick, %{count: count}} ->
          total = Map.get(ctx, :count, 0) + count
          send(test_pid, {:count, total})
          {:ok, %{count: total}}
        end,
        base_context: %{count: 1}
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:tick, %{count: 2}})
    assert_receive {:count, 3}

    assert :ok = Crank.notify({:tick, %{count: 5}})
    assert_receive {:count, 8}
  end

  test "subscribe/3 with rerun :wait coalesces to the latest signal" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "coalesce latest",
        fn _ctx, {:tick, %{value: value}} ->
          send(test_pid, {:started, self(), value})

          receive do
            {:release, ^value} -> :ok
          end

          send(test_pid, {:finished, value})
          :ok
        end,
        on_failure: :continue,
        rerun: :wait
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:tick, %{value: 1}})
    assert_receive {:started, run_pid_1, 1}

    assert :ok = Crank.notify({:tick, %{value: 2}})
    assert :ok = Crank.notify({:tick, %{value: 3}})

    send(run_pid_1, {:release, 1})

    assert_receive {:finished, 1}
    assert_receive {:started, run_pid_2, 3}
    refute_receive {:started, _, 2}, 200

    send(run_pid_2, {:release, 3})
    assert_receive {:finished, 3}
  end

  test "subscribe/3 with rerun {:kill, :graceful} restarts after stopping the active pipeline" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "graceful rerun",
        fn ctx, {:tick, %{value: value}} ->
          pipeline =
            Crank.new(ctx)
            |> Crank.step("sleep", "sleep 10")

          send(test_pid, {:planned_pipeline, value, pipeline.id, self()})

          receive do
            {:allow_run, ^value} ->
              {:ok, pipeline_id} = Crank.run(pipeline)

              send(test_pid, {:pipeline_started, value, pipeline_id})

              case Crank.await(pipeline_id, :infinity) do
                {:ok, new_ctx} -> {:ok, new_ctx}
                {:error, _reason, _ctx} -> :ok
              end
          end
        end,
        on_failure: :continue,
        rerun: {:kill, :graceful}
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:tick, %{value: 1}})
    assert_receive {:planned_pipeline, 1, pipeline_id_1, task_pid_1}
    Crank.Output.Test.subscribe(pipeline_id_1, self())
    send(task_pid_1, {:allow_run, 1})
    assert_receive {:pipeline_started, 1, ^pipeline_id_1}
    assert_receive {:crank_event, {:pipeline_started, ^pipeline_id_1, _}}, 5_000

    assert :ok = Crank.notify({:tick, %{value: 2}})

    assert_receive {:crank_event,
                    {:pipeline_failed, ^pipeline_id_1, %{reason: {:stopped, :graceful}}}},
                   5_000

    assert_receive {:planned_pipeline, 2, pipeline_id_2, task_pid_2}, 5_000
    Crank.Output.Test.subscribe(pipeline_id_2, self())
    send(task_pid_2, {:allow_run, 2})
    assert_receive {:pipeline_started, 2, ^pipeline_id_2}
    assert_receive {:crank_event, {:pipeline_started, ^pipeline_id_2, _}}, 5_000

    assert :ok = Crank.stop(pipeline_id_2, :graceful)

    assert_receive {:crank_event,
                    {:pipeline_failed, ^pipeline_id_2, %{reason: {:stopped, :graceful}}}},
                   5_000

    Crank.Output.Test.unsubscribe(pipeline_id_1)
    Crank.Output.Test.unsubscribe(pipeline_id_2)
  end

  test "subscribe/3 keeps the previous ctx when a pipeline run fails" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "ctx from pipeline success only",
        fn ctx, {:tick, %{mode: mode}} ->
          send(test_pid, {:seen_ctx, mode, ctx})

          case mode do
            :observe ->
              :ignore

            :success ->
              pipeline =
                Crank.new(ctx)
                |> Crank.step("success", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)

              {:ok, pipeline_id} = Crank.run(pipeline)

              case Crank.await(pipeline_id, :infinity) do
                {:ok, new_ctx} -> {:ok, new_ctx}
                {:error, _reason, _ctx} -> :ok
              end

            :fail ->
              pipeline =
                Crank.new(ctx)
                |> Crank.step("partial", fn _ctx, _opts -> {:ctx_add, %{partial: true}} end)
                |> Crank.step("boom", fn _ctx, _opts -> raise "boom" end)

              {:ok, pipeline_id} = Crank.run(pipeline)

              case Crank.await(pipeline_id, :infinity) do
                {:ok, new_ctx} -> {:ok, new_ctx}
                {:error, _reason, _ctx} -> :ok
              end
          end
        end,
        base_context: %{count: 1},
        on_failure: :continue
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:tick, %{mode: :fail}})
    assert_receive {:seen_ctx, :fail, %{count: 1}}

    assert :ok = Crank.notify({:tick, %{mode: :success}})
    assert_receive {:seen_ctx, :success, %{count: 1}}, 5_000

    assert :ok = Crank.notify({:tick, %{mode: :observe}})
    assert_receive {:seen_ctx, :observe, %{count: 1, done: true}}, 5_000
  end

  test "subscribed pipeline events include stable logical rerun metadata" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "meta pipeline",
        fn ctx, {:tick, %{run: run}} ->
          pipeline =
            Crank.new(ctx)
            |> Crank.step("meta", fn _ctx, _opts -> :ok end)

          send(test_pid, {:planned_pipeline, run, pipeline.id, self()})

          receive do
            {:allow_run, ^run} ->
              {:ok, pipeline_id} = Crank.run(pipeline)

              send(test_pid, {:pipeline_started, run, pipeline_id})

              case Crank.await(pipeline_id, :infinity) do
                {:ok, new_ctx} -> {:ok, new_ctx}
                {:error, _reason, _ctx} -> :ok
              end
          end
        end,
        on_failure: :continue
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:tick, %{run: 1}})
    assert_receive {:planned_pipeline, 1, pipeline_id_1, task_pid_1}
    Crank.Output.Test.subscribe(pipeline_id_1, self())
    send(task_pid_1, {:allow_run, 1})
    assert_receive {:pipeline_started, 1, ^pipeline_id_1}

    assert_receive {:crank_event,
                    {:pipeline_started, ^pipeline_id_1,
                     %{
                       meta: %{
                         crank: %{
                           run_n: 1,
                           subscription_name: "meta pipeline",
                           subscription_ref: ^subscription_ref
                         }
                       }
                     }}}

    Crank.Output.Test.unsubscribe(pipeline_id_1)

    assert :ok = Crank.notify({:tick, %{run: 2}})
    assert_receive {:planned_pipeline, 2, pipeline_id_2, task_pid_2}
    Crank.Output.Test.subscribe(pipeline_id_2, self())
    send(task_pid_2, {:allow_run, 2})
    assert_receive {:pipeline_started, 2, ^pipeline_id_2}

    assert_receive {:crank_event,
                    {:pipeline_started, ^pipeline_id_2,
                     %{
                       meta: %{
                         crank: %{
                           run_n: 2,
                           subscription_name: "meta pipeline",
                           subscription_ref: ^subscription_ref
                         }
                       }
                     }}}

    Crank.Output.Test.unsubscribe(pipeline_id_2)
  end

  test "subscription task does not hang when subscriber stops before pipeline start" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "handshake abort",
        fn ctx, {:tick, %{}} ->
          pipeline =
            Crank.new(ctx)
            |> Crank.step("gated", fn _ctx, _opts ->
              send(test_pid, {:step_running, self()})

              receive do
                :release_step -> :ok
              end
            end)

          send(test_pid, {:planned_pipeline, pipeline.id, self()})

          receive do
            :allow_run ->
              {:ok, _pipeline_id} = Crank.run(pipeline)
              :ok
          end
        end,
        on_failure: :continue
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    assert :ok = Crank.notify({:tick, %{}})
    assert_receive {:planned_pipeline, pipeline_id, task_pid}, 5_000
    Crank.Output.Test.subscribe(pipeline_id, self())
    on_exit(fn -> Crank.Output.Test.unsubscribe(pipeline_id) end)

    subscriber_pid = lookup_subscription_pid(subscription_ref)
    subscriber_ref = Process.monitor(subscriber_pid)
    task_ref = Process.monitor(task_pid)

    Process.exit(subscriber_pid, :shutdown)
    assert_receive {:DOWN, ^subscriber_ref, :process, ^subscriber_pid, _reason}, 5_000

    send(task_pid, :allow_run)

    assert_receive {:step_running, _step_pid}, 5_000
    assert_receive {:DOWN, ^task_ref, :process, ^task_pid, :normal}, 5_000
    assert_receive {:crank_event, {:pipeline_started, ^pipeline_id, _}}, 5_000

    assert_receive {:crank_event,
                    {:pipeline_failed, ^pipeline_id,
                     %{reason: {:stopped, {:owner_down, :normal}}}}},
                   5_000
  end

  test "unsubscribe/1 returns after requesting cancellation without waiting for cleanup" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "async unsubscribe",
        fn _ctx, {:tick, %{}} ->
          Process.flag(:trap_exit, true)
          send(test_pid, {:started, self()})

          receive do
            {:EXIT, _from, :shutdown} ->
              send(test_pid, :cleanup_started)

              receive do
                :finish_cleanup -> :ok
              end
          end
        end,
        on_failure: :continue
      )

    assert :ok = Crank.notify({:tick, %{}})
    assert_receive {:started, run_pid}, 5_000

    subscriber_pid = lookup_subscription_pid(subscription_ref)
    subscriber_ref = Process.monitor(subscriber_pid)

    assert :ok = Crank.unsubscribe(subscription_ref)
    assert_receive :cleanup_started, 5_000
    assert Process.alive?(subscriber_pid)
    refute_receive {:DOWN, ^subscriber_ref, :process, ^subscriber_pid, _reason}, 100

    send(run_pid, :finish_cleanup)

    assert_receive {:DOWN, ^subscriber_ref, :process, ^subscriber_pid, :normal}, 5_000
  end

  test "notify/1 raises on invalid signals" do
    assert_raise ArgumentError, fn ->
      Crank.notify(:not_a_signal)
    end
  end

  test "subscribe/3 validates in the caller without crashing the subscription subtree" do
    assert_raise ArgumentError, fn ->
      Crank.subscribe("invalid opts", fn _, _ -> :ok end, bogus: 1)
    end

    assert is_pid(Process.whereis(Crank.Subscriptions.SubscriberRegistry))
    assert is_pid(Process.whereis(Crank.Subscriptions.SubscriberSupervisor))
  end

  test "subscribe/3 with on_failure :continue survives callback crashes" do
    test_pid = self()

    subscription_ref =
      Crank.subscribe(
        "callback crash",
        fn _ctx, {:tick, %{mode: mode}} ->
          case mode do
            :crash ->
              raise "boom"

            :ok ->
              send(test_pid, :callback_recovered)
              :ok
          end
        end,
        on_failure: :continue
      )

    on_exit(fn -> Crank.unsubscribe(subscription_ref) end)

    ExUnit.CaptureLog.capture_log(fn ->
      assert :ok = Crank.notify({:tick, %{mode: :crash}})
    end)

    assert :ok = Crank.notify({:tick, %{mode: :ok}})
    assert_receive :callback_recovered, 5_000
  end

  @tag :tmp_dir
  test "watch/1 dispatches file system signals to subscribers", %{tmp_dir: tmp_dir} do
    test_pid = self()
    watch_path = Path.join(tmp_dir, "*")

    watch_ref = Crank.watch(watch_path)

    subscription_ref =
      Crank.subscribe(
        "watch notifications",
        fn _ctx, {:watch, %{watch: ref, changed: changed}} ->
          send(test_pid, {:watch_event, ref, changed})
          :ok
        end,
        on_failure: :continue
      )

    on_exit(fn ->
      Crank.unwatch(watch_ref)
      Crank.unsubscribe(subscription_ref)
    end)

    Process.sleep(100)
    watched_file = Path.join(tmp_dir, "watched.txt")
    assert :ok = File.write(watched_file, "hello")

    assert_receive {:watch_event, ^watch_ref, changed}, 5_000
    assert is_list(changed)
    assert Enum.all?(changed, &is_binary/1)
  end

  test "path specs treat literals as exact-only matches" do
    spec = PathSpec.new("/tmp/project")

    assert PathSpec.path(spec) == "/tmp/project"
    assert PathSpec.match?(spec, "/tmp/project")
    refute PathSpec.match?(spec, "/tmp/project/lib/file.ex")
    refute PathSpec.match?(spec, "/tmp/projected/file.ex")
  end

  test "path specs compile globs to their literal watch root" do
    spec = PathSpec.new("/tmp/project/**/*.ex")

    assert PathSpec.path(spec) == "/tmp/project"
    assert PathSpec.match?(spec, "/tmp/project/lib/file.ex")
    assert PathSpec.match?(spec, "/tmp/project/file.ex")
    refute PathSpec.match?(spec, "/tmp/project/file.txt")
  end

  test "path specs keep single-star globs within one directory level" do
    spec = PathSpec.new("/tmp/project/*.ex")

    assert PathSpec.match?(spec, "/tmp/project/file.ex")
    refute PathSpec.match?(spec, "/tmp/project/lib/file.ex")
  end

  test "path specs support brace alternation" do
    spec = PathSpec.new("/tmp/project/*.{ts,js}")

    assert PathSpec.path(spec) == "/tmp/project"
    assert PathSpec.match?(spec, "/tmp/project/app.ts")
    assert PathSpec.match?(spec, "/tmp/project/app.js")
    refute PathSpec.match?(spec, "/tmp/project/app.ex")
  end

  test "path specs can combine literal and glob filters" do
    path_specs = [
      PathSpec.new("/tmp/project/*.ex"),
      PathSpec.new("/tmp/project/always.md")
    ]

    assert Enum.any?(path_specs, &PathSpec.match?(&1, "/tmp/project/file.ex"))
    assert Enum.any?(path_specs, &PathSpec.match?(&1, "/tmp/project/always.md"))
    refute Enum.any?(path_specs, &PathSpec.match?(&1, "/tmp/project/other.md"))
  end

  @tag :tmp_dir
  test "path specs report validity for watch inputs", %{tmp_dir: tmp_dir} do
    watched_file = Path.join(tmp_dir, "file.txt")

    assert PathSpec.valid?(watched_file)
    assert PathSpec.valid?(Path.join(tmp_dir, "*"))
    assert PathSpec.valid?(Path.join(tmp_dir, "**"))
    refute PathSpec.valid?(tmp_dir)
    refute PathSpec.valid?(tmp_dir <> "/")
  end

  test "watch/1 raises a friendly error for invalid patterns" do
    assert_raise ArgumentError,
                 ~r/invalid watch pattern ".+\/\[broken": missing terminator/,
                 fn ->
                   Crank.watch("/tmp/[broken")
                 end
  end

  @tag :tmp_dir
  test "watch/1 rejects ambiguous bare directory paths", %{tmp_dir: tmp_dir} do
    assert_raise ArgumentError,
                 ~r/ambiguous watch path ".+": use ".+\/\*" or ".+\/\*\*"/,
                 fn ->
                   Crank.watch(tmp_dir)
                 end

    assert_raise ArgumentError,
                 ~r/ambiguous watch path ".+\/": use ".+\/\*" or ".+\/\*\*"/,
                 fn ->
                   Crank.watch(tmp_dir <> "/")
                 end
  end

  @tag :tmp_dir
  test "watch/1 treats glob inputs as filters", %{tmp_dir: tmp_dir} do
    test_pid = self()
    watch_path = Path.join(tmp_dir, "*.txt")
    ignored_file = Path.join(tmp_dir, "ignored.md")
    watched_file = Path.join(tmp_dir, "watched.txt")

    watch_ref = Crank.watch(watch_path)

    subscription_ref =
      Crank.subscribe(
        "glob watch notifications",
        fn _ctx, {:watch, %{watch: ref, changed: changed}} ->
          send(test_pid, {:watch_event, ref, changed})
          :ok
        end,
        on_failure: :continue
      )

    on_exit(fn ->
      Crank.unwatch(watch_ref)
      Crank.unsubscribe(subscription_ref)
    end)

    Process.sleep(100)

    assert :ok = File.write(ignored_file, "nope")
    ignored_changes = collect_watch_changes(watch_ref, 500)
    refute Enum.any?(ignored_changes, &(ignored_file in &1))

    assert :ok = File.write(watched_file, "hello")
    changed = await_watch_change(watch_ref, watched_file, 5_000)
    assert watched_file in changed
    refute ignored_file in changed
  end

  defp lookup_subscription_pid(subscription_ref) do
    [{pid, _}] = Registry.lookup(Crank.Subscriptions.SubscriberRegistry, subscription_ref)
    pid
  end

  defp collect_watch_changes(watch_ref, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_collect_watch_changes(watch_ref, deadline, [])
  end

  defp do_collect_watch_changes(watch_ref, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      Enum.reverse(acc)
    else
      receive do
        {:watch_event, ^watch_ref, changed} ->
          do_collect_watch_changes(watch_ref, deadline, [changed | acc])
      after
        remaining ->
          Enum.reverse(acc)
      end
    end
  end

  defp await_watch_change(watch_ref, changed_path, timeout) do
    receive do
      {:watch_event, ^watch_ref, changed} ->
        if changed_path in changed do
          changed
        else
          await_watch_change(watch_ref, changed_path, timeout)
        end
    after
      timeout ->
        flunk("expected watch event for #{inspect(changed_path)}")
    end
  end
end
