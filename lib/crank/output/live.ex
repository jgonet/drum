defmodule Crank.Output.Live do
  @moduledoc false
  @behaviour Crank.Output
  alias Crank.Output.Utils
  alias Crank.Utils, as: CrankUtils

  @spinner_frames ~w[⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏]

  @impl true
  def init(_opts) do
    %{
      pipeline_start_ms: nil,
      items: [],
      item_map: %{},
      group_children: %{},
      cmd_buffers: %{},
      cmd_seq: 0,
      successful_steps: 0,
      skipped_steps: 0,
      failed_step: nil,
      pipeline_start_time: nil,
      block_id: nil,
      ticker_tref: nil
    }
  end

  @impl true
  def handle_event({:pipeline_started, _, data}, state) do
    %{now_ms: now_ms, items: item_summaries} = data
    {_date, {h, m, _s}} = :calendar.local_time()
    start_time = "#{Utils.pad2(h)}:#{Utils.pad2(m)}"
    {items, item_map, group_children} = build_todo_state(item_summaries)

    id = make_ref()
    Owl.LiveScreen.add_block(id, state: state, render: &render_block/1)
    {:ok, tref} = :timer.send_interval(80, :live_tick)

    %{
      state
      | pipeline_start_ms: now_ms,
        pipeline_start_time: start_time,
        block_id: id,
        ticker_tref: tref,
        items: items,
        item_map: item_map,
        group_children: group_children
    }
  end

  def handle_event({:step_started, _, data}, state) do
    %{id: id, now_ms: now_ms} = data
    state = update_item(state, id, &%{&1 | state: :running, start_ms: now_ms})
    update_live(state)
  end

  def handle_event({:step_finished, _, data}, state) do
    %{id: id, now_ms: now_ms} = data
    state = update_item(state, id, &%{&1 | state: :ok, end_ms: now_ms})

    state = %{
      state
      | cmd_buffers: drop_step_buffers(state.cmd_buffers, id),
        successful_steps: state.successful_steps + 1
    }

    update_live(state)
  end

  def handle_event({:step_failed, _, data}, state) do
    %{id: id, name: name, reason: reason, now_ms: now_ms} = data
    state = update_item(state, id, &%{&1 | state: :failed, end_ms: now_ms, reason: reason})
    state = %{state | failed_step: name}
    update_live(state)
  end

  def handle_event({:step_skipped, _, data}, state) do
    %{id: id} = data
    state = update_item(state, id, &%{&1 | state: :skipped})
    state = %{state | skipped_steps: state.skipped_steps + 1}
    update_live(state)
  end

  def handle_event({:group_started, _, data}, state) do
    %{id: id, now_ms: now_ms} = data
    state = update_item(state, id, &%{&1 | state: :running, start_ms: now_ms})
    update_live(state)
  end

  def handle_event({:group_finished, _, data}, state) do
    %{id: id, now_ms: now_ms} = data
    state = update_item(state, id, &%{&1 | state: :ok, end_ms: now_ms})
    update_live(state)
  end

  def handle_event({:group_failed, _, data}, state) do
    %{id: id, reason: reason, now_ms: now_ms} = data
    state = update_item(state, id, &%{&1 | state: :failed, end_ms: now_ms, reason: reason})
    update_live(state)
  end

  def handle_event({:group_skipped, _, data}, state) do
    %{id: id} = data
    child_ids = Map.get(state.group_children, id, [])

    item_map =
      Enum.reduce(child_ids, state.item_map, fn child_id, acc ->
        Map.update!(acc, child_id, &%{&1 | state: :skipped})
      end)

    state = %{state | item_map: Map.update!(item_map, id, &%{&1 | state: :skipped})}
    update_live(state)
  end

  def handle_event({:command_started, _, data}, state) do
    %{id: id, cmd: cmd, step_id: step_id} = data

    buf = %{
      cmd: cmd,
      stderr: [],
      stdout: [],
      step_id: step_id,
      exit_code: nil,
      seq: state.cmd_seq
    }

    %{state | cmd_buffers: Map.put(state.cmd_buffers, id, buf), cmd_seq: state.cmd_seq + 1}
  end

  def handle_event({:command_stdout, _, data}, state) do
    %{id: id, data: chunk} = data
    update_cmd_buffer(state, id, &%{&1 | stdout: [chunk | &1.stdout]})
  end

  def handle_event({:command_stderr, _, data}, state) do
    %{id: id, data: chunk} = data
    update_cmd_buffer(state, id, &%{&1 | stderr: [chunk | &1.stderr]})
  end

  def handle_event({:command_finished, _, _data}, state), do: state

  def handle_event({:command_failed, _, data}, state) do
    %{id: id, exit_code: code} = data
    update_cmd_buffer(state, id, &%{&1 | exit_code: code})
  end

  def handle_event(:live_tick, state), do: update_live(state)

  def handle_event({:pipeline_finished, _, data}, state) do
    %{now_ms: now_ms} = data
    elapsed = elapsed_since(state.pipeline_start_ms, now_ms)
    stop_ticker(state.ticker_tref)
    flush_live(state)
    print_summary(state, elapsed, :ok)
    state
  end

  def handle_event({:pipeline_failed, _, data}, state) do
    %{now_ms: now_ms} = data
    elapsed = elapsed_since(state.pipeline_start_ms, now_ms)
    stop_ticker(state.ticker_tref)
    update_live(state)
    flush_live(state)
    print_summary(state, elapsed, :error)
    state
  end

  def handle_event(:terminate, state) do
    now_ms = CrankUtils.now_ms()

    item_map =
      Map.new(state.item_map, fn {id, item} ->
        item = if item.state == :running, do: %{item | state: :failed, end_ms: now_ms}, else: item
        {id, item}
      end)

    state = %{state | item_map: item_map}
    stop_ticker(state.ticker_tref)
    update_live(state)
    flush_live(state)
    state
  end

  def handle_event(_event, state), do: state

  # --- Rendering ---

  defp render_block(state) do
    now_ms = CrankUtils.now_ms()
    col = time_column(state)

    lines =
      Enum.flat_map(
        state.items,
        &render_item(Map.fetch!(state.item_map, &1), state, 0, now_ms, col)
      )

    Owl.Data.unlines(lines)
  end

  defp time_column(state) do
    max_prefix =
      state.items
      |> Enum.flat_map(&items_with_depth(Map.fetch!(state.item_map, &1), state, 0))
      |> Enum.reduce(0, fn {depth, name}, acc -> max(depth * 2 + 2 + String.length(name), acc) end)

    max_prefix + 2
  end

  defp items_with_depth(item, state, depth) do
    # Use max possible display name (total/total) so the time column never shifts as count grows
    own = [{depth, group_display_name(item, state, :max)}]

    if item.type == :group do
      children =
        state.group_children
        |> Map.get(item.id, [])
        |> Enum.flat_map(&items_with_depth(Map.fetch!(state.item_map, &1), state, depth + 1))

      own ++ children
    else
      own
    end
  end

  defp group_display_name(%{type: :group, name: name, id: id}, state, :max) do
    total = length(Map.get(state.group_children, id, []))
    "#{name} (#{total}/#{total})"
  end

  defp group_display_name(%{type: :group, name: name, id: id}, state, :current) do
    total = length(Map.get(state.group_children, id, []))
    done = count_done_children(state, id)
    "#{name} (#{done}/#{total})"
  end

  defp group_display_name(%{name: name}, _state, _mode), do: name

  defp count_done_children(state, group_id) do
    state.group_children
    |> Map.get(group_id, [])
    |> Enum.count(&(Map.fetch!(state.item_map, &1).state in [:ok, :failed, :skipped]))
  end

  defp render_item(item, state, depth, now_ms, col) do
    symbol = status_symbol(item.state, now_ms)
    time_tagged = format_elapsed(item, now_ms)
    indent = String.duplicate("  ", depth)
    display_name = group_display_name(item, state, :current)
    padding = String.duplicate(" ", max(col - (depth * 2 + 2 + String.length(display_name)), 1))
    line = [indent, symbol, " ", display_name, padding | time_tagged]

    cond do
      item.type == :group ->
        child_lines =
          state.group_children
          |> Map.get(item.id, [])
          |> Enum.flat_map(
            &render_item(Map.fetch!(state.item_map, &1), state, depth + 1, now_ms, col)
          )

        [line | child_lines]

      item.type == :step and item.state == :failed ->
        [line | render_cmd_lines(item, state, depth)]

      true ->
        [line]
    end
  end

  defp render_cmd_lines(item, state, depth) do
    cmd_indent = String.duplicate("  ", depth + 1)

    cmds =
      state.cmd_buffers
      |> Map.values()
      |> Enum.filter(&(&1.step_id == item.id))
      |> Enum.sort_by(& &1.seq)

    last_idx = length(cmds) - 1

    cmds
    |> Enum.with_index()
    |> Enum.flat_map(fn {buf, idx} ->
      is_last = idx == last_idx
      tree_char = if is_last, do: "└─", else: "├─"
      continuation = if is_last, do: "  ", else: "│ "

      failed = not is_nil(buf.exit_code)
      cmd_color = if failed, do: :red, else: :faint

      exit_tag =
        if failed, do: [Owl.Data.tag("  [exit #{buf.exit_code}]", :red)], else: []

      cmd_line = [cmd_indent, Owl.Data.tag("#{tree_char} $ #{buf.cmd}", [:italic, cmd_color]) | exit_tag]

      output_indent = cmd_indent <> continuation <> "  "
      stderr_lines = render_output_lines(buf.stderr, output_indent, [:faint, :red])
      stdout_lines = render_output_lines(buf.stdout, output_indent, :faint)

      [cmd_line] ++ stderr_lines ++ stdout_lines
    end)
  end

  defp render_output_lines([], _indent, _color), do: []

  defp render_output_lines(chunks, indent, color) do
    chunks
    |> Enum.reverse()
    |> Enum.flat_map(fn chunk ->
      chunk
      |> String.trim_trailing("\n")
      |> String.split("\n")
      |> Enum.map(&[indent, Owl.Data.tag(&1, color)])
    end)
  end

  defp status_symbol(:todo, _), do: Owl.Data.tag("○", :light_black)
  defp status_symbol(:ok, _), do: Owl.Data.tag("✓", :green)
  defp status_symbol(:skipped, _), do: Owl.Data.tag("◆", :yellow)
  defp status_symbol(:failed, _), do: Owl.Data.tag("✕", :red)

  defp status_symbol(:running, _) do
    idx = rem(div(System.monotonic_time(:millisecond), 100), length(@spinner_frames))
    Owl.Data.tag(Enum.at(@spinner_frames, idx), :faint)
  end

  defp format_elapsed(%{state: :running, start_ms: start_ms}, now_ms) when is_integer(start_ms) do
    [" ", Owl.Data.tag(Utils.format_duration(now_ms - start_ms), :faint)]
  end

  defp format_elapsed(%{state: status, start_ms: start_ms, end_ms: end_ms}, _now_ms)
       when status in [:ok, :failed] and is_integer(start_ms) and is_integer(end_ms) do
    [" ", Owl.Data.tag(Utils.format_duration(end_ms - start_ms), :faint)]
  end

  defp format_elapsed(_, _), do: []

  # --- State helpers ---

  defp new_item(id, type, name, status, start_ms) do
    %{id: id, type: type, name: name, state: status, start_ms: start_ms, end_ms: nil, reason: nil}
  end

  defp build_todo_state(item_summaries) do
    Enum.reduce(item_summaries, {[], %{}, %{}}, fn item, {ids, item_map, group_children} ->
      todo = new_item(item.id, item.type, item.name, :todo, nil)
      item_map = Map.put(item_map, item.id, todo)
      ids = ids ++ [item.id]

      if item.type == :group do
        child_ids = Enum.map(item.steps, & &1.id)

        child_item_map =
          Map.new(item.steps, &{&1.id, new_item(&1.id, :step, &1.name, :todo, nil)})

        {ids, Map.merge(item_map, child_item_map), Map.put(group_children, item.id, child_ids)}
      else
        {ids, item_map, group_children}
      end
    end)
  end

  defp update_item(state, id, f) do
    case Map.fetch(state.item_map, id) do
      {:ok, item} -> %{state | item_map: Map.put(state.item_map, id, f.(item))}
      :error -> state
    end
  end

  defp stop_ticker(nil), do: :ok
  defp stop_ticker(tref), do: :timer.cancel(tref)

  defp update_live(%{block_id: nil} = state), do: state

  defp update_live(state) do
    Owl.LiveScreen.update(state.block_id, state)
    state
  end

  defp flush_live(%{block_id: nil}), do: :ok

  defp flush_live(_state) do
    Owl.LiveScreen.await_render()
    Owl.LiveScreen.flush()
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

  defp print_summary(state, elapsed, :ok) do
    parts = [
      "#{state.successful_steps} ok, #{state.skipped_steps} skipped",
      "started #{state.pipeline_start_time}",
      "#{Utils.format_duration(elapsed)} total"
    ]

    IO.puts("")
    IO.puts("✓  " <> Enum.join(parts, "  ·  "))
  end

  defp print_summary(state, elapsed, :error) do
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

    IO.puts("")
    IO.puts("✕  " <> Enum.join(parts, "  ·  "))
  end
end
