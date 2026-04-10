defmodule Crank.Output.Plain do
  @moduledoc false
  @behaviour Crank.Output
  alias Crank.Output.Utils

  @impl true
  def init(opts) do
    %{
      pipeline_start_ms: nil,
      pipeline_start_time: nil,
      step_starts: %{},
      group_starts: %{},
      group_totals: %{},
      group_done: %{},
      cmd_buffers: %{},
      cmd_seq: 0,
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
    start_time = utc_timestamp()
    state.print.("Start (#{start_time})")
    %{state | pipeline_start_ms: now_ms, pipeline_start_time: start_time}
  end

  def handle_event({:step_started, _, data}, state) do
    %{id: id, name: name, group_id: group_id, now_ms: now_ms} = data
    state.print.("#{step_prefix(group_id)}>> #{name}")
    %{state | step_starts: Map.put(state.step_starts, id, now_ms)}
  end

  def handle_event({:step_finished, _, data}, state) do
    %{id: id, name: name, group_id: group_id, now_ms: now_ms} = data
    elapsed = now_ms - Map.fetch!(state.step_starts, id)
    state.print.("#{step_prefix(group_id)}ok #{name}  #{Utils.format_duration(elapsed)}")

    state = %{
      state
      | cmd_buffers: drop_step_buffers(state.cmd_buffers, id),
        successful_steps: state.successful_steps + 1
    }

    inc_group_done(state, group_id)
  end

  def handle_event({:step_failed, _, data}, state) do
    %{id: id, name: name, group_id: group_id, reason: reason, now_ms: now_ms} = data
    elapsed = now_ms - Map.get(state.step_starts, id, now_ms)

    state.print.(
      "#{step_prefix(group_id)}FAIL #{name}  #{Utils.format_duration(elapsed)}  (#{format_step_reason(reason)})"
    )

    flush_detail_block(id, group_id, state.cmd_buffers, state.print)

    state = %{
      state
      | cmd_buffers: drop_step_buffers(state.cmd_buffers, id),
        failed_step: name,
        failed_step_elapsed: elapsed
    }

    inc_group_done(state, group_id)
  end

  def handle_event({:step_skipped, _, data}, state) do
    %{name: name, group_id: group_id} = data
    state.print.("#{step_prefix(group_id)}skip #{name}")
    state = %{state | skipped_steps: state.skipped_steps + 1}
    inc_group_done(state, group_id)
  end

  def handle_event({:group_started, _, data}, state) do
    %{id: id, name: name, step_count: step_count, now_ms: now_ms} = data
    state.print.(">> #{name} (#{step_count} #{Utils.pluralize(step_count, "step")})")

    %{
      state
      | group_starts: Map.put(state.group_starts, id, now_ms),
        group_totals: Map.put(state.group_totals, id, step_count),
        group_done: Map.put(state.group_done, id, 0)
    }
  end

  def handle_event({:group_finished, _, data}, state) do
    %{id: id, name: name, now_ms: now_ms} = data
    elapsed = now_ms - Map.fetch!(state.group_starts, id)
    {done, total} = group_count(state, id)
    state.print.("ok #{name} (#{done}/#{total})  #{Utils.format_duration(elapsed)}")
    %{state | group_starts: Map.delete(state.group_starts, id)}
  end

  def handle_event({:group_failed, _, data}, state) do
    %{id: id, name: name, now_ms: now_ms} = data
    elapsed = now_ms - Map.get(state.group_starts, id, now_ms)
    {done, total} = group_count(state, id)
    state.print.("FAIL #{name} (#{done}/#{total})  #{Utils.format_duration(elapsed)}")
    %{state | group_starts: Map.delete(state.group_starts, id)}
  end

  def handle_event({:group_skipped, _, data}, state) do
    %{name: name} = data
    state.print.("skip #{name}")
    state
  end

  def handle_event({:command_started, _, data}, state) do
    %{id: id, cmd: cmd, step_id: step_id} = data
    buf = %{cmd: cmd, stderr: [], stdout: [], step_id: step_id, exit_code: nil, seq: state.cmd_seq}
    %{state | cmd_buffers: Map.put(state.cmd_buffers, id, buf), cmd_seq: state.cmd_seq + 1}
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
      "#{state.successful_steps} ok, #{state.skipped_steps} skipped",
      "started #{state.pipeline_start_time}",
      "#{Utils.format_duration(elapsed)} total"
    ]

    state.print.("")
    state.print.("OK: " <> Enum.join(parts, " | "))
    state
  end

  def handle_event({:pipeline_failed, _, data}, state) do
    %{now_ms: now_ms} = data
    elapsed = elapsed_since(state.pipeline_start_ms, now_ms)

    failed_label =
      case state.failed_step do
        nil -> "failed"
        name -> "failed #{name}"
      end

    parts = [
      failed_label,
      "#{state.successful_steps} ok, #{state.skipped_steps} skipped",
      "started #{state.pipeline_start_time}",
      "#{Utils.format_duration(elapsed)} total"
    ]

    state.print.("")
    state.print.("FAIL: " <> Enum.join(parts, " | "))
    state
  end

  def handle_event(_event, state), do: state

  defp step_prefix(nil), do: ""
  defp step_prefix(_group_id), do: "  "

  defp flush_detail_block(step_id, group_id, cmd_buffers, print) do
    cmd_prefix = if group_id, do: "    ", else: "  "
    out_prefix = cmd_prefix <> "  "

    cmd_buffers
    |> Map.values()
    |> Enum.filter(&(&1.step_id == step_id))
    |> Enum.sort_by(& &1.seq)
    |> Enum.each(fn buf ->
      exit_suffix = if buf.exit_code, do: "  [exit #{buf.exit_code}]", else: ""
      print.("#{cmd_prefix}$ #{buf.cmd}#{exit_suffix}")

      [buf.stderr, buf.stdout]
      |> Enum.each(fn chunks ->
        chunks
        |> Enum.reverse()
        |> Enum.each(fn chunk ->
          chunk
          |> String.trim_trailing("\n")
          |> String.split("\n")
          |> Enum.each(&IO.write("#{out_prefix}#{&1}\n"))
        end)
      end)
    end)
  end

  defp group_count(state, group_id) do
    total = Map.get(state.group_totals, group_id, "?")
    done = Map.get(state.group_done, group_id, total)
    {done, total}
  end

  defp inc_group_done(state, nil), do: state

  defp inc_group_done(state, group_id) do
    %{state | group_done: Map.update(state.group_done, group_id, 1, &(&1 + 1))}
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

  defp utc_timestamp do
    {{year, month, day}, {h, m, s}} = :calendar.universal_time()
    "#{year}-#{Utils.pad2(month)}-#{Utils.pad2(day)}T#{Utils.pad2(h)}:#{Utils.pad2(m)}:#{Utils.pad2(s)}Z"
  end

  defp format_step_reason({:action_error, %Crank.CommandError{exit_code: code}}),
    do: "exit code: #{code}"

  defp format_step_reason(:timeout), do: "timeout"
  defp format_step_reason({:action_error, _e}), do: "exception"
  defp format_step_reason(_), do: "failed"
end
