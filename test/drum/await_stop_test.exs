defmodule Drum.AwaitStopTest do
  use ExUnit.Case, async: true
  import Drum.Test.PipelineHelpers

  describe "await/2" do
    test "await/2 returns the final ctx on success" do
      pipeline_id =
        Drum.new(%{start: :ok})
        |> Drum.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
        |> Drum.run()

      assert {:ok, %{start: :ok, done: true}} = Drum.await(pipeline_id)
    end

    test "await/2 returns the failure reason and last ctx" do
      pipeline_id =
        Drum.new(%{start: :ok})
        |> Drum.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
        |> Drum.step("boom", fn _ctx, _opts -> raise "boom" end)
        |> Drum.run()

      assert {:error, {:action_error, %RuntimeError{message: "boom"}}, %{start: :ok, done: true}} =
               Drum.await(pipeline_id)
    end

    test "await/2 works for late awaiters in the owner process and consumes the result" do
      {:ok, pipeline_id} =
        Drum.new()
        |> Drum.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
        |> run_pipeline()

      assert {:ok, _data} = await_pipeline(pipeline_id)
      assert {:ok, %{done: true}} = Drum.await(pipeline_id)
      assert {:error, :timeout} = Drum.await(pipeline_id, 0)
    end

    test "await/2 with a list returns results in input order" do
      test_pid = self()

      slow_pipeline =
        gated_step(Drum.new(), "slow", test_pid,
          tag: :slow_step,
          result: {:ctx_add, %{name: :slow}}
        )

      fast_pipeline =
        Drum.new()
        |> Drum.step("fast", fn _ctx, _opts -> {:ctx_add, %{name: :fast}} end)

      fast_id = fast_pipeline.id
      Drum.Output.Test.subscribe(fast_id, self())
      ExUnit.Callbacks.on_exit(fn -> Drum.Output.Test.unsubscribe(fast_id) end)

      slow_id = Drum.run(slow_pipeline)
      ^fast_id = Drum.run(fast_pipeline)

      assert_receive {:slow_step, slow_step_pid}
      assert {:ok, _data} = await_pipeline(fast_id)
      send(slow_step_pid, :release)

      assert [
               {:ok, %{name: :fast}},
               {:ok, %{name: :slow}}
             ] = Drum.await([fast_id, slow_id], 1_000)
    end

    test "await/2 timeout does not consume the later result" do
      test_pid = self()

      pipeline_id =
        gated_step(Drum.new(), "wait", test_pid, result: {:ctx_add, %{done: true}})
        |> Drum.run()

      assert_receive {:step_pid, step_pid}
      assert {:error, :timeout} = Drum.await(pipeline_id, 0)
      send(step_pid, :release)
      assert {:ok, %{done: true}} = Drum.await(pipeline_id)
    end

    test "await/2 is owner-scoped" do
      test_pid = self()

      owner =
        spawn(fn ->
          pipeline = gated_step(Drum.new(), "wait", test_pid, result: {:ctx_add, %{done: true}})
          pipeline_id = Drum.run(pipeline)
          send(test_pid, {:pipeline_id, pipeline_id})

          receive do
            :await ->
              send(test_pid, {:owner_result, Drum.await(pipeline_id)})
          end
        end)

      assert is_pid(owner)
      assert_receive {:pipeline_id, pipeline_id}
      assert_receive {:step_pid, step_pid}
      assert {:error, :timeout} = Drum.await(pipeline_id, 0)
      send(step_pid, :release)
      send(owner, :await)
      assert_receive {:owner_result, {:ok, %{done: true}}}
    end
  end

  describe "stop/2" do
    test "stop/2 gracefully stops an active pipeline" do
      test_pid = self()

      pipeline_id =
        gated_step(Drum.new(), "wait", test_pid)
        |> Drum.run()

      assert_receive {:step_pid, step_pid}
      assert :ok = Drum.stop(pipeline_id, :graceful)
      assert {:error, {:stopped, :graceful}, %{}} = Drum.await(pipeline_id)
      assert_process_down(step_pid)
    end
  end

  describe "pipeline ownership" do
    test "owner exit stops the pipeline" do
      test_pid = self()

      pipeline = gated_step(Drum.new(), "wait", test_pid)

      pipeline_id = pipeline.id
      Drum.Output.Test.subscribe(pipeline_id, self())
      ExUnit.Callbacks.on_exit(fn -> Drum.Output.Test.unsubscribe(pipeline_id) end)

      owner =
        spawn(fn ->
          send(test_pid, {:run_result, Drum.Pipeline.start_pipeline(pipeline, owner: self())})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:run_result, {:ok, ^pipeline_id}}
      assert_receive {:step_pid, step_pid}

      pipeline_pid = Drum.Registry.lookup_pipeline(pipeline_id)
      assert is_pid(pipeline_pid)

      send(owner, :stop)

      assert_receive {:drum_event,
                      {:pipeline_failed, ^pipeline_id,
                       %{reason: {:stopped, {:owner_down, :normal}}}}}

      assert_process_down(step_pid)
      assert_process_down(pipeline_pid)
    end
  end
end
