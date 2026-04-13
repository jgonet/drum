defmodule Crank.OutputServerFailureTest do
  use ExUnit.Case, async: false

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

    {:ok, pipeline_id} = Crank.run(pipeline)
    pipeline_pid = Crank.Registry.lookup_pipeline(pipeline_id)

    assert is_pid(pipeline_pid)
    assert_receive {:step_pid, step_pid}

    output_pid = Process.whereis(Crank.Output.Server)
    output_ref = Process.monitor(output_pid)
    Process.exit(output_pid, :boom)

    assert_receive {:DOWN, ^output_ref, :process, ^output_pid, :boom}
    assert_process_down(step_pid)
    assert_process_down(pipeline_pid)
    assert {:error, :timeout} = Crank.await(pipeline_id, 50)
  end

  defp assert_process_down(pid, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_process_down(pid, deadline)
  end

  defp do_assert_process_down(pid, deadline) do
    if not Process.alive?(pid) do
      :ok
    else
      remaining = deadline - System.monotonic_time(:millisecond)
      assert remaining > 0
      Process.sleep(10)
      do_assert_process_down(pid, deadline)
    end
  end
end
