defmodule Crank.OutputServerFailureTest do
  use ExUnit.Case, async: false
  import Crank.Test.PipelineHelpers

  setup do
    wait_for_process(Crank.Registry)
    wait_for_process(Crank.Output.Server)
    wait_for_process(Crank.PipelinesSup)
    wait_for_process(Crank.Subscriptions.RunRegistry)
    :ok
  end

  defmodule ShutdownProbe do
    @behaviour Crank.Output

    def init(opts) do
      %{notify: Keyword.fetch!(opts, :notify)}
    end

    def handle_event(:terminate, state) do
      send(state.notify, :output_server_terminated)
      state
    end

    def handle_event(_event, state), do: state
  end

  test "output server crash stops an active pipeline" do
    test_pid = self()

    pipeline =
      Crank.new()
      |> Crank.step("wait", fn _ctx, _opts ->
        send(test_pid, {:step_pid, self()})

        receive do
          :release -> :ok
        end
      end)

    pipeline_id = Crank.run(pipeline)
    pipeline_pid = Crank.Registry.lookup_pipeline(pipeline_id)

    assert is_pid(pipeline_pid)
    assert_receive {:step_pid, step_pid}

    output_pid = Process.whereis(Crank.Output.Server)
    output_ref = Process.monitor(output_pid)
    Process.exit(output_pid, :kill)

    assert_receive {:DOWN, ^output_ref, :process, ^output_pid, :killed}
    assert_process_down(step_pid)
    assert_process_down(pipeline_pid)
    assert {:error, :timeout} = Crank.await(pipeline_id, 0)
  end

  test "output server traps exits and calls the adapter terminate hook on supervisor shutdown" do
    child = %{
      id: :test_output_server,
      restart: :temporary,
      start:
        {GenServer, :start_link, [Crank.Output.Server, {ShutdownProbe, [notify: self()]}, []]},
      type: :worker
    }

    {:ok, supervisor_pid} = Supervisor.start_link([child], strategy: :one_for_one)

    [{:test_output_server, output_pid, :worker, [GenServer]}] =
      Supervisor.which_children(supervisor_pid)

    assert {:trap_exit, true} = Process.info(output_pid, :trap_exit)
    assert :ok = Supervisor.stop(supervisor_pid)
    assert_receive :output_server_terminated
  end

  test "await waits for terminal output events to be processed" do
    test_pid = self()

    pipeline_id =
      gated_step(Crank.new(), "wait", test_pid)
      |> Crank.run()

    Crank.Output.Test.subscribe(pipeline_id, self())

    on_exit(fn ->
      Crank.Output.Test.unsubscribe(pipeline_id)
      resume_output_server()
    end)

    assert_receive {:step_pid, step_pid}
    :ok = :sys.suspend(Crank.Output.Server)
    send(step_pid, :release)

    assert {:error, :timeout} = Crank.await(pipeline_id, 50)

    :ok = :sys.resume(Crank.Output.Server)

    assert {:ok, %{}} = Crank.await(pipeline_id, 5_000)
    assert_received {:crank_event, {:step_finished, ^pipeline_id, _data}}
    assert_received {:crank_event, {:pipeline_finished, ^pipeline_id, _data}}
  end

  defp resume_output_server do
    :sys.resume(Crank.Output.Server)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp wait_for_process(name, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_for_process(name, deadline)
  end

  defp do_wait_for_process(name, deadline) do
    if is_pid(Process.whereis(name)) do
      :ok
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        flunk("process #{inspect(name)} did not start within timeout")
      else
        Process.sleep(min(remaining, 10))
        do_wait_for_process(name, deadline)
      end
    end
  end
end
