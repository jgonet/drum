defmodule Crank.AwaitStopTest do
  use ExUnit.Case, async: true
  import Crank.Test.PipelineHelpers

  describe "await/2" do
    test "await/2 returns the final ctx on success" do
      pipeline_id =
        Crank.new(%{start: :ok})
        |> Crank.step("add", fn _ctx, _opts -> {:ctx_add, %{done: true}} end)
        |> Crank.run()

      assert {:ok, %{start: :ok, done: true}} = Crank.await(pipeline_id)
    end

    test "await/2 returns the failure reason and last ctx" do
      pipeline_id =
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
        gated_step(Crank.new(), "slow", test_pid, tag: :slow_step, result: {:ctx_add, %{name: :slow}})

      fast_pipeline =
        Crank.new()
        |> Crank.step("fast", fn _ctx, _opts -> {:ctx_add, %{name: :fast}} end)

      fast_id = fast_pipeline.id
      Crank.Output.Test.subscribe(fast_id, self())
      ExUnit.Callbacks.on_exit(fn -> Crank.Output.Test.unsubscribe(fast_id) end)

      slow_id = Crank.run(slow_pipeline)
      ^fast_id = Crank.run(fast_pipeline)

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

      pipeline_id =
        gated_step(Crank.new(), "wait", test_pid, result: {:ctx_add, %{done: true}})
        |> Crank.run()

      assert_receive {:step_pid, step_pid}
      assert {:error, :timeout} = Crank.await(pipeline_id, 0)
      send(step_pid, :release)
      assert {:ok, %{done: true}} = Crank.await(pipeline_id)
    end

    test "await/2 is owner-scoped" do
      test_pid = self()

      owner =
        spawn(fn ->
          pipeline = gated_step(Crank.new(), "wait", test_pid, result: {:ctx_add, %{done: true}})
          pipeline_id = Crank.run(pipeline)
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
  end

  describe "stop/2" do
    test "stop/2 gracefully stops an active pipeline" do
      test_pid = self()

      pipeline_id =
        gated_step(Crank.new(), "wait", test_pid)
        |> Crank.run()

      assert_receive {:step_pid, step_pid}
      assert :ok = Crank.stop(pipeline_id, :graceful)
      assert {:error, {:stopped, :graceful}, %{}} = Crank.await(pipeline_id)
      assert_process_down(step_pid)
    end
  end

  describe "pipeline ownership" do
    test "owner exit stops the pipeline" do
      test_pid = self()

      pipeline = gated_step(Crank.new(), "wait", test_pid)

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
  end
end
