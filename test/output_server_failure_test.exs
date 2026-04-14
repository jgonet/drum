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
    assert {:error, :timeout} = Crank.await(pipeline_id, 0)
  end

  defp assert_process_down(pid, timeout \\ 1_000) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, timeout
  end
end
