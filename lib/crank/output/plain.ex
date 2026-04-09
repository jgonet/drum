defmodule Crank.Output.Plain do
  @moduledoc false
  @behaviour Crank.Output
  alias Crank.Output.Utils

  @impl true
  def init(opts) do
    %{
      pipeline_start_ms: nil,
      step_starts: %{},
      group_starts: %{},
      cmd_buffers: %{},
      successful_steps: 0,
      skipped_steps: 0,
      failed_step: nil,
      failed_step_elapsed: nil,
      print: Keyword.get(opts, :print, &IO.puts/1)
    }
  end

  @impl true
  def handle_event({:pipeline_started, _, data}, state) do
    %{now_ms: now_ms} = data
    {_date, {h, m, _s}} = :calendar.local_time()
    state.print.("Start (#{Utils.pad2(h)}:#{Utils.pad2(m)})")
    %{state | pipeline_start_ms: now_ms}
  end

  def handle_event({:step_started, _, data}, state) do
    %{id: id, name: name, group_id: group_id, now_ms: now_ms} = data
    state.print.("#{step_prefix(group_id)}start #{name}")

    %{
      state
      | step_starts: Map.put(state.step_starts, id, now_ms)
    }
  end

  def handle_event({:step_finished, _, data}, state) do
    %{id: id, name: name, group_id: group_id, now_ms: now_ms} = data
    elapsed = now_ms - Map.fetch!(state.step_starts, id)
    state.print.("#{step_prefix(group_id)}ok #{name} (#{Utils.format_duration(elapsed)})")

    %{
      state
      | cmd_buffers: drop_step_buffers(state.cmd_buffers, id),
        successful_steps: state.successful_steps + 1
    }
  end

  def handle_event({:step_failed, _, data}, state) do
    %{id: id, name: name, group_id: group_id, reason: reason, now_ms: now_ms} = data
    elapsed = now_ms - Map.get(state.step_starts, id, now_ms)

    state.print.(
      "#{step_prefix(group_id)}error #{name} (#{Utils.format_duration(elapsed)}, #{format_step_reason(reason)})"
    )

    flush_detail_block(id, group_id, state.cmd_buffers, state.print)

    %{state | cmd_buffers: drop_step_buffers(state.cmd_buffers, id), failed_step: name, failed_step_elapsed: elapsed}
  end

  def handle_event({:step_skipped, _, data}, state) do
    %{name: name, group_id: group_id} = data
    state.print.("#{step_prefix(group_id)}skipped #{name}")
    %{state | skipped_steps: state.skipped_steps + 1}
  end

  def handle_event({:group_started, _, data}, state) do
    %{id: id, name: name, step_count: step_count, now_ms: now_ms} = data
    state.print.("- start #{name} (#{step_count} #{Utils.pluralize(step_count, "step")})")
    %{state | group_starts: Map.put(state.group_starts, id, now_ms)}
  end

  def handle_event({:group_finished, _, data}, state) do
    %{id: id, name: name, now_ms: now_ms} = data
    elapsed = now_ms - Map.fetch!(state.group_starts, id)
    state.print.("- ok #{name} (#{Utils.format_duration(elapsed)})")
    %{state | group_starts: Map.delete(state.group_starts, id)}
  end

  def handle_event({:group_failed, _, data}, state) do
    %{id: id, name: name, now_ms: now_ms} = data
    elapsed = now_ms - Map.get(state.group_starts, id, now_ms)
    state.print.("- error #{name} (#{Utils.format_duration(elapsed)})")
    %{state | group_starts: Map.delete(state.group_starts, id)}
  end

  def handle_event({:group_skipped, _, data}, state) do
    %{name: name} = data
    state.print.("- skipped #{name}")
    state
  end

  def handle_event({:command_started, _, data}, state) do
    %{id: id, cmd: cmd, step_id: step_id} = data
    buf = %{cmd: cmd, stderr: [], stdout: [], step_id: step_id, exit_code: nil}
    %{state | cmd_buffers: Map.put(state.cmd_buffers, id, buf)}
  end

  def handle_event({:command_stdout, _, data}, state) do
    %{id: id, data: chunk} = data
    update_cmd_buffer(state, id, fn buf -> %{buf | stdout: [chunk | buf.stdout]} end)
  end

  def handle_event({:command_stderr, _, data}, state) do
    %{id: id, data: chunk} = data
    update_cmd_buffer(state, id, fn buf -> %{buf | stderr: [chunk | buf.stderr]} end)
  end

  def handle_event({:command_finished, _, _data}, state), do: state

  def handle_event({:command_failed, _, data}, state) do
    %{id: id, exit_code: code} = data
    update_cmd_buffer(state, id, fn buf -> %{buf | exit_code: code} end)
  end

  def handle_event({:pipeline_finished, _, data}, state) do
    %{now_ms: now_ms} = data
    elapsed = elapsed_since(state.pipeline_start_ms, now_ms)

    parts = [
      "#{state.successful_steps} successful #{Utils.pluralize(state.successful_steps, "step")}",
      if(state.skipped_steps > 0, do: "#{state.skipped_steps} skipped", else: nil)
    ]

    state.print.("#{Utils.format_parts(parts)} (#{Utils.format_duration(elapsed)})")
    state
  end

  def handle_event({:pipeline_failed, _, data}, state) do
    %{now_ms: now_ms} = data
    elapsed = elapsed_since(state.pipeline_start_ms, now_ms)

    failed_part =
      case {state.failed_step, state.failed_step_elapsed} do
        {nil, _} -> "failed"
        {name, nil} -> "failed #{name}"
        {name, step_elapsed} -> "failed #{name} in #{Utils.format_duration(step_elapsed)}"
      end

    parts = [
      "#{state.successful_steps} successful #{Utils.pluralize(state.successful_steps, "step")}",
      if(state.skipped_steps > 0, do: "#{state.skipped_steps} skipped", else: nil),
      failed_part
    ]

    state.print.("#{Utils.format_parts(parts)} (#{Utils.format_duration(elapsed)} total)")
    state
  end

  def handle_event(_event, state), do: state

  defp step_prefix(nil), do: "- "
  defp step_prefix(_group_id), do: "  - "

  defp detail_prefix(nil), do: "    "
  defp detail_prefix(_group_id), do: "      "

  defp flush_detail_block(step_id, group_id, cmd_buffers, print) do
    prefix = detail_prefix(group_id)

    cmd_buffers
    |> Map.values()
    |> Enum.filter(&(&1.step_id == step_id))
    |> Enum.each(fn buf ->
      print.("#{prefix}- `#{buf.cmd}`")

      if buf.stderr != [] do
        print.("#{prefix}stderr:")
        buf.stderr |> Enum.reverse() |> Enum.each(&IO.write("#{prefix}#{&1}"))
      end

      if buf.stdout != [] do
        print.("#{prefix}stdout:")
        buf.stdout |> Enum.reverse() |> Enum.each(&IO.write("#{prefix}#{&1}"))
      end
    end)
  end

  defp drop_step_buffers(cmd_buffers, step_id) do
    Map.reject(cmd_buffers, fn {_id, buf} -> buf.step_id == step_id end)
  end

  defp update_cmd_buffer(state, cmd_id, f) do
    case Map.fetch(state.cmd_buffers, cmd_id) do
      {:ok, buf} -> %{state | cmd_buffers: Map.put(state.cmd_buffers, cmd_id, f.(buf))}
      :error -> state
    end
  end

  defp elapsed_since(nil, now_ms), do: now_ms
  defp elapsed_since(start_ms, now_ms), do: now_ms - start_ms

  defp format_step_reason({:action_error, %Crank.CommandError{exit_code: code}}),
    do: "exit code: #{code}"

  defp format_step_reason(:timeout), do: "timeout"
  defp format_step_reason({:action_error, _e}), do: "exception"
  defp format_step_reason(_), do: "failed"

end
