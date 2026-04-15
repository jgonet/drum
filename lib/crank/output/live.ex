defmodule Crank.Output.Live do
  @moduledoc false
  @behaviour Crank.Output

  alias Crank.TmpDir
  alias Crank.Output.Utils
  alias Crank.Utils, as: CrankUtils

  @spinner_frames ~w[⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏]
  @restart_source_highlight_ms 30_000
  @run_refresh_ms 80
  @watch_pattern_wrap_limit 60
  @watch_summary_limit 2

  @impl true
  def init(_opts) do
    %{
      block_id: nil,
      cards: %{},
      cards_order: [],
      next_card_label_index: 1,
      next_watch_label_index: 1,
      pipeline_to_card: %{},
      refresh_mode: nil,
      refresh_ref: nil,
      refresh_value: nil,
      watchers: %{},
      watchers_order: []
    }
  end

  @impl true
  def handle_event({:watcher_registered, watch_ref, data}, state) do
    %{patterns: patterns, roots: roots} = data
    state = ensure_watcher(state, watch_ref, patterns, roots)
    push_render(state)
  end

  def handle_event({:watcher_updated, watch_ref, data}, state) do
    %{changed: changed, now_ms: now_ms} = data
    state = ensure_watcher(state, watch_ref, [], [])

    state =
      update_watcher(state, watch_ref, fn watcher ->
        %{watcher | last_changed: changed, last_update_ms: now_ms}
      end)

    push_render(state)
  end

  def handle_event({:watcher_removed, watch_ref, _data}, state) do
    state = remove_watcher(state, watch_ref)
    push_render(state)
  end

  def handle_event({:ui_pipeline_registered, card_key, data}, state) do
    state = ensure_card(state, card_key, Map.get(data, :label))
    push_render(state)
  end

  def handle_event({:ui_pipeline_signal, card_key, data}, state) do
    %{now_ms: now_ms, pending: pending, signal: signal} = data
    state = ensure_card(state, card_key, nil)
    fallback_label = signal_label(signal, state, nil)

    state =
      update_card(state, card_key, fn card ->
        signals = if pending, do: clear_pending_signals(card.signals), else: card.signals
        signal_key = signal_key(signal)

        signal_state = %{
          fallback_label: fallback_label,
          last_seen_ms: now_ms,
          pending: pending,
          signal: signal
        }

        %{card | last_activity_ms: now_ms, signals: Map.put(signals, signal_key, signal_state)}
      end)

    push_render(state)
  end

  def handle_event({:ui_pipeline_restarting, card_key, data}, state) do
    %{mode: mode, now_ms: now_ms, signal: signal} = data
    state = ensure_card(state, card_key, nil)

    state =
      update_card(state, card_key, fn card ->
        restart_reason = %{
          fallback_label: signal_label(signal, state, nil),
          mode: mode,
          signal: signal,
          triggered_at_ms: now_ms
        }

        %{
          card
          | header_state: :restarting,
            last_activity_ms: now_ms,
            restart_reason: restart_reason
        }
      end)

    push_render(state)
  end

  def handle_event({:ui_pipeline_run_started, card_key, data}, state) do
    %{now_ms: now_ms, pipeline_id: pipeline_id, run_n: run_n} = data
    state = ensure_card(state, card_key, nil)

    state =
      state
      |> put_pipeline_card(pipeline_id, card_key)
      |> update_card(card_key, fn card ->
        %{
          card
          | active_pipeline_id: pipeline_id,
            active_run_n: run_n,
            header_state: nil,
            last_activity_ms: now_ms,
            restart_reason: card.restart_reason,
            run_count: max(card.run_count, run_n),
            signals: clear_pending_signals(card.signals)
        }
      end)

    push_render(state)
  end

  def handle_event({:pipeline_started, pipeline_id, data}, state) do
    %{items: item_summaries, meta: meta, now_ms: now_ms} = data
    crank_meta = Map.get(meta, :crank, %{})
    card_key = Map.get(crank_meta, :logical_id, {:pipeline, pipeline_id})
    label = Map.get(crank_meta, :subscription_name)
    run_n = Map.get(crank_meta, :run_n)
    state = ensure_card(state, card_key, label)

    run_state = new_run_state(pipeline_id, item_summaries, now_ms)

    state =
      state
      |> put_pipeline_card(pipeline_id, card_key)
      |> update_card(card_key, fn card ->
        %{
          card
          | active_pipeline_id: pipeline_id,
            active_run_n: run_n,
            header_state: nil,
            last_activity_ms: now_ms,
            restart_reason: card.restart_reason,
            run_count: max(card.run_count, run_count_for_run(run_n)),
            run_state: run_state,
            signals: clear_pending_signals(card.signals)
        }
      end)

    push_render(state)
  end

  def handle_event({:step_started, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        update_item(run_state, id, &%{&1 | start_ms: now_ms, state: :running})
      end)

    push_render(state)
  end

  def handle_event({:step_finished, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        run_state = update_item(run_state, id, &%{&1 | end_ms: now_ms, state: :ok})

        %{
          run_state
          | cmd_buffers: drop_step_buffers(run_state.cmd_buffers, id),
            successful_steps: run_state.successful_steps + 1
        }
      end)

    push_render(state)
  end

  def handle_event({:step_failed, pipeline_id, data}, state) do
    %{id: id, name: name, now_ms: now_ms, reason: reason} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        run_state =
          update_item(run_state, id, &%{&1 | end_ms: now_ms, reason: reason, state: :failed})

        %{run_state | failed_step: name}
      end)

    push_render(state)
  end

  def handle_event({:step_skipped, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        run_state = update_item(run_state, id, &%{&1 | state: :skipped})
        %{run_state | skipped_steps: run_state.skipped_steps + 1}
      end)

    push_render(state)
  end

  def handle_event({:group_started, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        update_item(run_state, id, &%{&1 | start_ms: now_ms, state: :running})
      end)

    push_render(state)
  end

  def handle_event({:group_finished, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        update_item(run_state, id, &%{&1 | end_ms: now_ms, state: :ok})
      end)

    push_render(state)
  end

  def handle_event({:group_failed, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms, reason: reason} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        update_item(run_state, id, &%{&1 | end_ms: now_ms, reason: reason, state: :failed})
      end)

    push_render(state)
  end

  def handle_event({:group_skipped, pipeline_id, data}, state) do
    %{id: id, now_ms: now_ms} = data

    state =
      update_card_run_state(state, pipeline_id, now_ms, fn run_state ->
        child_ids = Map.get(run_state.group_children, id, [])

        item_map =
          Enum.reduce(child_ids, run_state.item_map, fn child_id, acc ->
            Map.update!(acc, child_id, &%{&1 | state: :skipped})
          end)

        next_item_map = Map.update!(item_map, id, &%{&1 | state: :skipped})

        %{
          run_state
          | item_map: next_item_map,
            skipped_steps: run_state.skipped_steps + length(child_ids)
        }
      end)

    push_render(state)
  end

  def handle_event({:command_started, pipeline_id, data}, state) do
    %{cmd: cmd, id: id, step_id: step_id} = data

    update_card_run_state(state, pipeline_id, CrankUtils.now_ms(), fn run_state ->
      buffer = %{
        cmd: cmd,
        exit_code: nil,
        seq: run_state.cmd_seq,
        stderr: [],
        stdout: [],
        step_id: step_id
      }

      %{
        run_state
        | cmd_buffers: Map.put(run_state.cmd_buffers, id, buffer),
          cmd_seq: run_state.cmd_seq + 1
      }
    end)
  end

  def handle_event({:command_stdout, pipeline_id, data}, state) do
    %{data: chunk, id: id} = data

    update_card_run_state(
      state,
      pipeline_id,
      CrankUtils.now_ms(),
      &update_cmd_buffer(&1, id, fn buffer -> %{buffer | stdout: [chunk | buffer.stdout]} end)
    )
  end

  def handle_event({:command_stderr, pipeline_id, data}, state) do
    %{data: chunk, id: id} = data

    update_card_run_state(
      state,
      pipeline_id,
      CrankUtils.now_ms(),
      &update_cmd_buffer(&1, id, fn buffer -> %{buffer | stderr: [chunk | buffer.stderr]} end)
    )
  end

  def handle_event({:command_finished, _pipeline_id, _data}, state), do: state

  def handle_event({:command_failed, pipeline_id, data}, state) do
    %{exit_code: exit_code, id: id} = data

    update_card_run_state(
      state,
      pipeline_id,
      CrankUtils.now_ms(),
      &update_cmd_buffer(&1, id, fn buffer -> %{buffer | exit_code: exit_code} end)
    )
  end

  def handle_event({:pipeline_finished, pipeline_id, data}, state) do
    %{now_ms: now_ms} = data

    state =
      state
      |> update_card_run_state(pipeline_id, now_ms, fn run_state ->
        %{run_state | status: :ok}
      end)
      |> clear_active_pipeline(pipeline_id, now_ms)

    push_render(state)
  end

  def handle_event({:pipeline_failed, pipeline_id, data}, state) do
    %{now_ms: now_ms, reason: reason} = data

    state =
      state
      |> update_card_run_state(pipeline_id, now_ms, fn run_state ->
        stop_reason = stop_reason_for_pipeline_failure(state, pipeline_id, reason)
        finalize_run_state(run_state, now_ms, stop_reason)
      end)
      |> clear_active_pipeline(pipeline_id, now_ms, reason)

    push_render(state)
  end

  def handle_event(:live_tick, state), do: push_render(state)

  def handle_event(:live_refresh, state) do
    push_render(%{state | refresh_mode: nil, refresh_ref: nil, refresh_value: nil})
  end

  def handle_event(:terminate, state) do
    now_ms = CrankUtils.now_ms()

    cards =
      Map.new(state.cards, fn {card_key, card} ->
        updated_card =
          if is_map(card.run_state) do
            %{
              card
              | active_pipeline_id: nil,
                active_run_n: nil,
                run_state: finalize_run_state(card.run_state, now_ms, nil)
            }
          else
            card
          end

        {card_key, updated_card}
      end)

    state = %{state | cards: cards}
    state = cancel_refresh(state)
    state = push_render(state)
    flush_live(state)
    state
  end

  def handle_event(_event, state), do: state

  @doc false
  def render_dashboard(state, now_ms \\ CrankUtils.now_ms()) do
    watcher_lines = render_watchers(state, now_ms)

    card_lines =
      state
      |> ordered_cards()
      |> Enum.flat_map(&render_card(&1, state, now_ms))

    lines =
      []
      |> maybe_append_section("Watchers", watcher_lines)
      |> maybe_append_lines(card_lines)

    Owl.Data.unlines(lines)
  end

  @doc false
  def format_relative_age(last_seen_ms, now_ms)

  def format_relative_age(nil, _now_ms), do: "never"

  def format_relative_age(last_seen_ms, now_ms) when is_integer(last_seen_ms) do
    age_ms = max(now_ms - last_seen_ms, 0)

    cond do
      age_ms < 30_000 ->
        "now"

      age_ms < 300_000 ->
        "#{format_relative_duration(div(age_ms, 30_000) * 30_000)} ago"

      age_ms < 600_000 ->
        "#{format_relative_duration(div(age_ms, 60_000) * 60_000)} ago"

      true ->
        "#{format_relative_duration(div(age_ms, 120_000) * 120_000)} ago"
    end
  end

  @doc false
  def next_relative_age_delay(last_seen_ms, now_ms)

  def next_relative_age_delay(nil, _now_ms), do: nil

  def next_relative_age_delay(last_seen_ms, now_ms) when is_integer(last_seen_ms) do
    age_ms = max(now_ms - last_seen_ms, 0)

    {bucket_ms, raw_delay} =
      cond do
        age_ms < 30_000 ->
          {30_000, 30_000 - age_ms}

        age_ms < 300_000 ->
          {30_000, 30_000 - rem(age_ms, 30_000)}

        age_ms < 600_000 ->
          {60_000, 60_000 - rem(age_ms, 60_000)}

        true ->
          {120_000, 120_000 - rem(age_ms, 120_000)}
      end

    if raw_delay == 0 do
      bucket_ms
    else
      raw_delay
    end
  end

  @doc false
  def summarize_changed_paths(changed_paths, roots)

  def summarize_changed_paths([], _roots), do: nil

  def summarize_changed_paths([path], roots) do
    display_changed_path(path, roots)
  end

  def summarize_changed_paths(changed_paths, roots) do
    changed_paths
    |> Enum.map(&display_grouped_path(&1, roots))
    |> Enum.group_by(&top_level_segment/1)
    |> Enum.map(fn {top_level, paths} ->
      second_level =
        paths
        |> Enum.map(&second_level_segment/1)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      bucket =
        case {top_level, second_level} do
          {nil, _second_level} ->
            hd(paths)

          {top_level, []} ->
            "#{top_level}/"

          {top_level, [segment]} ->
            "#{top_level}/#{segment}/"

          {top_level, _second_level} ->
            "#{top_level}/"
        end

      {bucket, length(paths)}
    end)
    |> Enum.sort_by(fn {bucket, count} -> {-count, bucket} end)
    |> format_grouped_paths()
  end

  defp render_watchers(state, now_ms) do
    state.watchers_order
    |> Enum.filter(&Map.has_key?(state.watchers, &1))
    |> Enum.map(&render_watcher(Map.fetch!(state.watchers, &1), now_ms))
    |> join_line_groups()
  end

  defp render_watcher(watcher, now_ms) do
    heading_line =
      join_with_dot([watcher_label(watcher), age_tag(watcher.last_update_ms, now_ms)])

    watching_lines = render_watching_lines(watcher.patterns)

    changed_lines =
      case summarize_changed_paths(watcher.last_changed, watcher.roots) do
        nil -> []
        changed_summary -> [["  last: ", changed_summary]]
      end

    [heading_line] ++ watching_lines ++ changed_lines
  end

  defp render_card(card, state, now_ms) do
    if renderable_card?(card) do
      title =
        case card.header_state do
          :restarting ->
            join_with_dot([bold(card_label(card)), Owl.Data.tag("restarting", :faint)])

          _ ->
            bold(card_label(card))
        end

      signal_lines = render_signal_lines(card, state, now_ms)
      run_lines = render_run_lines(card.run_state, now_ms)

      body_lines =
        cond do
          signal_lines != [] and run_lines != [] ->
            signal_lines ++ [""] ++ run_lines

          signal_lines != [] ->
            signal_lines

          true ->
            run_lines
        end

      render_box(title, body_lines)
    else
      []
    end
  end

  defp render_signal_lines(card, state, now_ms) do
    card.signals
    |> Map.values()
    |> Enum.sort_by(fn signal_state ->
      {-Map.get(signal_state, :last_seen_ms, 0),
       signal_label(signal_state.signal, state, signal_state.fallback_label)}
    end)
    |> Enum.map(fn signal_state ->
      label = signal_display_label(card, signal_state, state, now_ms)
      pending = if signal_state.pending, do: "pending", else: nil

      run_count =
        if card.run_count > 0,
          do: "#{card.run_count} #{Utils.pluralize(card.run_count, "run")}",
          else: nil

      age = age_tag(signal_state.last_seen_ms, now_ms)
      join_with_dot([label, pending, run_count, age])
    end)
  end

  defp render_run_lines(nil, _now_ms), do: []

  defp render_run_lines(run_state, now_ms) do
    time_column = time_column(run_state)

    Enum.flat_map(run_state.items, fn item_id ->
      run_state.item_map
      |> Map.fetch!(item_id)
      |> render_item(run_state, 0, now_ms, time_column)
    end)
  end

  defp render_item(item, run_state, depth, now_ms, time_column) do
    indent = String.duplicate("  ", depth)
    display_name = group_display_name(item, run_state, :current)
    prefix_width = depth * 2 + 2 + String.length(display_name)
    padding = String.duplicate(" ", max(time_column - prefix_width, 1))

    line = [
      indent,
      status_symbol(item.state, now_ms),
      " ",
      display_name,
      padding | format_elapsed(item, now_ms)
    ]

    cond do
      item.type == :group ->
        child_lines =
          run_state.group_children
          |> Map.get(item.id, [])
          |> Enum.flat_map(fn child_id ->
            render_item(
              Map.fetch!(run_state.item_map, child_id),
              run_state,
              depth + 1,
              now_ms,
              time_column
            )
          end)

        [line | child_lines]

      item.type == :step and item.state == :failed ->
        [line] ++ render_stop_reason_line(item, depth) ++ render_cmd_lines(item, run_state, depth)

      true ->
        [line]
    end
  end

  defp render_stop_reason_line(%{stop_reason: nil}, _depth), do: []

  defp render_stop_reason_line(%{stop_reason: reason_label}, depth) do
    indent = String.duplicate("  ", depth + 1)
    [[indent, Owl.Data.tag("(stopped by #{reason_label})", :faint)]]
  end

  defp render_cmd_lines(item, run_state, depth) do
    cmd_indent = String.duplicate("  ", depth + 1)

    commands =
      run_state.cmd_buffers
      |> Map.values()
      |> Enum.filter(&(&1.step_id == item.id))
      |> Enum.sort_by(& &1.seq)

    last_index = length(commands) - 1

    commands
    |> Enum.with_index()
    |> Enum.flat_map(fn {buffer, index} ->
      is_last = index == last_index
      tree_char = if is_last, do: "└─", else: "├─"
      continuation = if is_last, do: "  ", else: "│ "
      failed = not is_nil(buffer.exit_code)
      cmd_color = if failed, do: :red, else: :faint

      exit_tag =
        if failed do
          [Owl.Data.tag("  [exit #{buffer.exit_code}]", :red)]
        else
          []
        end

      command_line = [
        cmd_indent,
        Owl.Data.tag("#{tree_char} $ #{buffer.cmd}", [:italic, cmd_color]) | exit_tag
      ]

      output_indent = cmd_indent <> continuation <> "  "
      stderr_lines = render_output_lines(buffer.stderr, output_indent, [:faint, :red])
      stdout_lines = render_output_lines(buffer.stdout, output_indent, :faint)

      [command_line] ++ stderr_lines ++ stdout_lines
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

  defp new_run_state(pipeline_id, item_summaries, now_ms) do
    {items, item_map, group_children} = build_todo_state(item_summaries)

    %{
      cmd_buffers: %{},
      cmd_seq: 0,
      failed_step: nil,
      group_children: group_children,
      item_map: item_map,
      items: items,
      pipeline_id: pipeline_id,
      pipeline_start_ms: now_ms,
      skipped_steps: 0,
      status: :running,
      successful_steps: 0
    }
  end

  defp build_todo_state(item_summaries) do
    Enum.reduce(item_summaries, {[], %{}, %{}}, fn item, {ids, item_map, group_children} ->
      todo_item = new_item(item.id, item.type, item.name)
      next_item_map = Map.put(item_map, item.id, todo_item)
      next_ids = ids ++ [item.id]

      if item.type == :group do
        child_ids = Enum.map(item.steps, & &1.id)

        child_item_map =
          Map.new(item.steps, fn step ->
            {step.id, new_item(step.id, :step, step.name)}
          end)

        {next_ids, Map.merge(next_item_map, child_item_map),
         Map.put(group_children, item.id, child_ids)}
      else
        {next_ids, next_item_map, group_children}
      end
    end)
  end

  defp new_item(id, type, name) do
    %{
      end_ms: nil,
      id: id,
      name: name,
      reason: nil,
      start_ms: nil,
      state: :todo,
      stop_reason: nil,
      type: type
    }
  end

  defp update_item(run_state, id, fun) do
    case Map.fetch(run_state.item_map, id) do
      {:ok, item} -> %{run_state | item_map: Map.put(run_state.item_map, id, fun.(item))}
      :error -> run_state
    end
  end

  defp update_cmd_buffer(run_state, cmd_id, fun) do
    case Map.fetch(run_state.cmd_buffers, cmd_id) do
      {:ok, buffer} ->
        %{run_state | cmd_buffers: Map.put(run_state.cmd_buffers, cmd_id, fun.(buffer))}

      :error ->
        run_state
    end
  end

  defp drop_step_buffers(cmd_buffers, step_id) do
    Map.reject(cmd_buffers, fn {_cmd_id, buffer} -> buffer.step_id == step_id end)
  end

  defp finalize_run_state(run_state, now_ms, stop_reason) do
    next_item_map =
      Map.new(run_state.item_map, fn {id, item} ->
        updated_item =
          if item.state == :running do
            next_stop_reason = if item.type == :step, do: stop_reason, else: nil

            %{
              item
              | end_ms: now_ms,
                reason: {:stopped, :graceful},
                state: :failed,
                stop_reason: next_stop_reason
            }
          else
            item
          end

        {id, updated_item}
      end)

    failed_step =
      run_state.failed_step ||
        next_item_map
        |> Map.values()
        |> Enum.find_value(fn item ->
          if item.type == :step and item.state == :failed and is_binary(item.stop_reason) do
            item.name
          end
        end)

    %{run_state | failed_step: failed_step, item_map: next_item_map, status: :failed}
  end

  defp status_symbol(:todo, _now_ms), do: Owl.Data.tag("○", :light_black)
  defp status_symbol(:ok, _now_ms), do: Owl.Data.tag("✓", :green)
  defp status_symbol(:skipped, _now_ms), do: Owl.Data.tag("◆", :yellow)
  defp status_symbol(:failed, _now_ms), do: Owl.Data.tag("✕", :red)

  defp status_symbol(:running, _now_ms) do
    frame_index = rem(div(System.monotonic_time(:millisecond), 100), length(@spinner_frames))
    Owl.Data.tag(Enum.at(@spinner_frames, frame_index), :faint)
  end

  defp format_elapsed(%{start_ms: start_ms, state: :running}, now_ms) when is_integer(start_ms) do
    [" ", Owl.Data.tag(Utils.format_duration(now_ms - start_ms), :faint)]
  end

  defp format_elapsed(%{end_ms: end_ms, start_ms: start_ms, state: state}, _now_ms)
       when state in [:ok, :failed] and is_integer(start_ms) and is_integer(end_ms) do
    [" ", Owl.Data.tag(Utils.format_duration(end_ms - start_ms), :faint)]
  end

  defp format_elapsed(_item, _now_ms), do: []

  defp time_column(run_state) do
    run_state.items
    |> Enum.flat_map(fn item_id ->
      items_with_depth(Map.fetch!(run_state.item_map, item_id), run_state, 0)
    end)
    |> Enum.reduce(0, fn {depth, name}, acc ->
      max(depth * 2 + 2 + String.length(name), acc)
    end)
    |> Kernel.+(2)
  end

  defp items_with_depth(item, run_state, depth) do
    own = [{depth, group_display_name(item, run_state, :max)}]

    if item.type == :group do
      children =
        run_state.group_children
        |> Map.get(item.id, [])
        |> Enum.flat_map(fn child_id ->
          items_with_depth(Map.fetch!(run_state.item_map, child_id), run_state, depth + 1)
        end)

      own ++ children
    else
      own
    end
  end

  defp group_display_name(%{id: id, name: name, type: :group}, run_state, :max) do
    total = length(Map.get(run_state.group_children, id, []))
    "#{name} (#{total}/#{total})"
  end

  defp group_display_name(%{id: id, name: name, type: :group}, run_state, :current) do
    total = length(Map.get(run_state.group_children, id, []))

    done =
      run_state.group_children
      |> Map.get(id, [])
      |> Enum.count(fn child_id ->
        Map.fetch!(run_state.item_map, child_id).state in [:failed, :ok, :skipped]
      end)

    "#{name} (#{done}/#{total})"
  end

  defp group_display_name(%{name: name}, _run_state, _mode), do: name

  defp ordered_cards(state) do
    state.cards_order
    |> Enum.filter(&Map.has_key?(state.cards, &1))
    |> Enum.map(&Map.fetch!(state.cards, &1))
    |> Enum.filter(&renderable_card?/1)
  end

  defp renderable_card?(card) do
    card.run_state != nil or map_size(card.signals) > 0
  end

  defp ensure_card(state, card_key, label) do
    case Map.fetch(state.cards, card_key) do
      {:ok, card} ->
        if is_binary(label) and is_nil(card.label) do
          update_card(state, card_key, &%{&1 | label: label})
        else
          state
        end

      :error ->
        generated_index =
          if is_binary(label) do
            nil
          else
            state.next_card_label_index
          end

        card = %{
          active_pipeline_id: nil,
          active_run_n: nil,
          card_key: card_key,
          generated_index: generated_index,
          header_state: nil,
          label: label,
          last_activity_ms: nil,
          restart_reason: nil,
          run_count: 0,
          run_state: nil,
          signals: %{}
        }

        %{
          state
          | cards: Map.put(state.cards, card_key, card),
            cards_order: state.cards_order ++ [card_key],
            next_card_label_index:
              if(is_nil(generated_index),
                do: state.next_card_label_index,
                else: generated_index + 1
              )
        }
    end
  end

  defp ensure_watcher(state, watch_ref, patterns, roots) do
    case Map.fetch(state.watchers, watch_ref) do
      {:ok, watcher} ->
        next_watcher = %{
          watcher
          | patterns: if(patterns == [], do: watcher.patterns, else: patterns),
            roots: if(roots == [], do: watcher.roots, else: roots)
        }

        %{state | watchers: Map.put(state.watchers, watch_ref, next_watcher)}

      :error ->
        watcher = %{
          generated_index: state.next_watch_label_index,
          last_changed: [],
          last_update_ms: nil,
          patterns: patterns,
          roots: roots,
          watch_ref: watch_ref
        }

        %{
          state
          | watchers: Map.put(state.watchers, watch_ref, watcher),
            watchers_order: state.watchers_order ++ [watch_ref],
            next_watch_label_index: state.next_watch_label_index + 1
        }
    end
  end

  defp update_card(state, card_key, fun) do
    case Map.fetch(state.cards, card_key) do
      {:ok, card} -> %{state | cards: Map.put(state.cards, card_key, fun.(card))}
      :error -> state
    end
  end

  defp update_watcher(state, watch_ref, fun) do
    case Map.fetch(state.watchers, watch_ref) do
      {:ok, watcher} -> %{state | watchers: Map.put(state.watchers, watch_ref, fun.(watcher))}
      :error -> state
    end
  end

  defp remove_watcher(state, watch_ref) do
    %{
      state
      | watchers: Map.delete(state.watchers, watch_ref),
        watchers_order: Enum.reject(state.watchers_order, &(&1 == watch_ref))
    }
  end

  defp update_card_run_state(state, pipeline_id, now_ms, fun) do
    case Map.fetch(state.pipeline_to_card, pipeline_id) do
      {:ok, card_key} ->
        update_card(state, card_key, fn card ->
          next_run_state =
            case card.run_state do
              nil -> nil
              run_state -> fun.(run_state)
            end

          %{card | last_activity_ms: now_ms, run_state: next_run_state}
        end)

      :error ->
        state
    end
  end

  defp clear_active_pipeline(state, pipeline_id, now_ms, reason \\ nil) do
    case Map.fetch(state.pipeline_to_card, pipeline_id) do
      {:ok, card_key} ->
        next_state =
          update_card(state, card_key, fn card ->
            keep_restarting = reason == {:stopped, :graceful} and card.header_state == :restarting

            %{
              card
              | active_pipeline_id: nil,
                active_run_n: nil,
                header_state: if(keep_restarting, do: card.header_state, else: nil),
                last_activity_ms: now_ms,
                restart_reason: card.restart_reason
            }
          end)

        %{next_state | pipeline_to_card: Map.delete(next_state.pipeline_to_card, pipeline_id)}

      :error ->
        state
    end
  end

  defp put_pipeline_card(state, pipeline_id, card_key) do
    %{state | pipeline_to_card: Map.put(state.pipeline_to_card, pipeline_id, card_key)}
  end

  defp clear_pending_signals(signals) do
    Map.new(signals, fn {signal_key, signal_state} ->
      {signal_key, %{signal_state | pending: false}}
    end)
  end

  defp stop_reason_for_pipeline_failure(state, pipeline_id, {:stopped, :graceful}) do
    case Map.fetch(state.pipeline_to_card, pipeline_id) do
      {:ok, card_key} ->
        case Map.fetch(state.cards, card_key) do
          {:ok, %{header_state: :restarting, restart_reason: %{fallback_label: label}}} -> label
          _ -> nil
        end

      :error ->
        nil
    end
  end

  defp stop_reason_for_pipeline_failure(_state, _pipeline_id, _reason), do: nil

  defp watcher_label(watcher), do: "watch##{watcher.generated_index}"

  defp card_label(%{label: label}) when is_binary(label), do: label
  defp card_label(%{generated_index: index}), do: "Pipeline ##{index}"

  defp bold(text), do: Owl.Data.tag(text, :bright)

  defp age_tag(last_seen_ms, now_ms) do
    Owl.Data.tag("last " <> format_relative_age(last_seen_ms, now_ms), :faint)
  end

  defp signal_key({:watch, %{watch: watch_ref}}) when is_reference(watch_ref),
    do: {:watch, watch_ref}

  defp signal_key({type, data}) when is_atom(type) and is_map(data) do
    {:signal, type, Map.get(data, :label) || Map.get(data, :name)}
  end

  defp signal_key(other), do: {:signal, other}

  defp signal_label({:watch, %{watch: watch_ref}}, state, fallback_label) do
    case Map.fetch(state.watchers, watch_ref) do
      {:ok, watcher} -> watcher_label(watcher)
      :error -> fallback_label || "watch"
    end
  end

  defp signal_label({type, data}, _state, _fallback_label) when is_atom(type) and is_map(data) do
    cond do
      is_binary(Map.get(data, :label)) ->
        Map.fetch!(data, :label)

      is_binary(Map.get(data, :name)) ->
        Map.fetch!(data, :name)

      true ->
        type
        |> Atom.to_string()
        |> String.replace("_", " ")
    end
  end

  defp signal_label(_signal, _state, fallback_label), do: fallback_label || "signal"

  defp signal_display_label(card, signal_state, state, now_ms) do
    label = signal_label(signal_state.signal, state, signal_state.fallback_label)

    if restart_source?(card, signal_state, now_ms) do
      Owl.Data.tag(label, :underline)
    else
      label
    end
  end

  defp restart_source?(card, signal_state, now_ms) do
    case card.restart_reason do
      %{signal: signal, triggered_at_ms: triggered_at_ms}
      when is_integer(triggered_at_ms) and is_integer(signal_state.last_seen_ms) ->
        signal_key(signal) == signal_key(signal_state.signal) and
          signal_state.last_seen_ms == triggered_at_ms and
          now_ms - triggered_at_ms < @restart_source_highlight_ms

      _ ->
        false
    end
  end

  defp render_watching_lines(patterns) do
    patterns
    |> Enum.map(&display_watch_pattern/1)
    |> wrap_prefixed_items("  watching: ", @watch_pattern_wrap_limit)
  end

  defp display_watch_pattern(pattern) do
    if Path.type(pattern) == :relative do
      pattern
    else
      pattern
      |> Path.expand()
      |> compact_display_path()
    end
  end

  defp compact_display_path(path) do
    case roll_managed_tmp_path(path) do
      nil -> replace_home_prefix(path)
      rolled_path -> rolled_path
    end
  end

  defp roll_managed_tmp_path(path) do
    tmp_dirs_root = TmpDir.tmp_dirs_root()

    if within_root?(path, tmp_dirs_root) do
      case Path.split(Path.relative_to(path, tmp_dirs_root)) do
        [dir_name | rest] ->
          dir_label = "tmp-#{dir_name}"

          if rest == [] do
            dir_label
          else
            Path.join(rest) <> " in " <> dir_label
          end

        _ ->
          nil
      end
    else
      nil
    end
  end

  defp replace_home_prefix(path) do
    home = System.user_home!()

    if within_root?(path, home) do
      relative_path = Path.relative_to(path, home)

      if relative_path == "." do
        "~"
      else
        Path.join("~", relative_path)
      end
    else
      path
    end
  end

  defp wrap_prefixed_items([], _prefix, _line_limit), do: []

  defp wrap_prefixed_items(items, prefix, line_limit) do
    continuation = String.duplicate(" ", String.length(prefix))

    {lines, current_line} =
      Enum.reduce(items, {[], prefix}, fn item, {lines, current_line} ->
        separator = if current_line == prefix, do: "", else: ", "
        candidate = current_line <> separator <> item

        if String.length(candidate) <= line_limit or current_line == prefix do
          {lines, candidate}
        else
          {lines ++ [current_line <> ","], continuation <> item}
        end
      end)

    lines ++ [current_line]
  end

  defp display_changed_path(path, roots) do
    expanded_path = Path.expand(path)

    case best_matching_root(expanded_path, roots) do
      nil ->
        compact_display_path(expanded_path)

      root ->
        relative = Path.relative_to(expanded_path, root)

        if relative == "." do
          Path.basename(expanded_path)
        else
          relative
        end
    end
  end

  defp display_grouped_path(path, roots) do
    expanded_path = Path.expand(path)

    case best_matching_root(expanded_path, roots) do
      nil ->
        compact_display_path(expanded_path)

      root ->
        relative = Path.relative_to(expanded_path, root)

        if relative == "." do
          Path.basename(expanded_path)
        else
          root_basename = Path.basename(root)

          if String.contains?(relative, "/") or root_basename in ["", "."] do
            relative
          else
            Path.join(root_basename, relative)
          end
        end
    end
  end

  defp best_matching_root(path, roots) do
    roots
    |> Enum.map(&Path.expand/1)
    |> Enum.filter(&within_root?(path, &1))
    |> Enum.max_by(&String.length/1, fn -> nil end)
  end

  defp top_level_segment(path) do
    path
    |> String.split("/", trim: true)
    |> List.first()
  end

  defp second_level_segment(path) do
    case String.split(path, "/", trim: true) do
      [_top, second, _rest | _tail] -> second
      _ -> nil
    end
  end

  defp format_grouped_paths(grouped_paths) do
    {shown, hidden} = Enum.split(grouped_paths, @watch_summary_limit)

    shown_summary =
      shown
      |> Enum.map(fn {bucket, count} -> "#{bucket} (#{count})" end)
      |> Enum.join(", ")

    case hidden do
      [] -> shown_summary
      hidden -> Enum.join([shown_summary, "+#{length(hidden)} more dirs"], ", ")
    end
  end

  defp within_root?(path, root) do
    path_segments = Path.split(Path.expand(path))
    root_segments = Path.split(Path.expand(root))
    Enum.take(path_segments, length(root_segments)) == root_segments
  end

  defp maybe_append_section(lines, _title, []), do: lines

  defp maybe_append_section(lines, title, body_lines) do
    lines ++ render_box(bold(title), body_lines)
  end

  defp maybe_append_lines(lines, []), do: lines
  defp maybe_append_lines(lines, new_lines), do: lines ++ new_lines

  defp render_box(title, body_lines) do
    top_line = ["╭─ ", title]

    content_lines =
      Enum.map(body_lines, fn
        "" -> "│"
        line -> ["│ ", line]
      end)

    [top_line | content_lines] ++ ["┴"]
  end

  defp join_line_groups([]), do: []

  defp join_line_groups(groups) do
    Enum.reduce(groups, [], fn group, lines ->
      if lines == [] do
        group
      else
        lines ++ [""] ++ group
      end
    end)
  end

  defp join_with_dot(parts) do
    parts
    |> Enum.reject(&is_nil/1)
    |> Enum.intersperse(Owl.Data.tag(" · ", :faint))
  end

  defp run_count_for_run(run_n) when is_integer(run_n), do: run_n
  defp run_count_for_run(_run_n), do: 1

  defp format_relative_duration(age_ms) do
    age_ms
    |> Utils.format_duration()
    |> String.replace(~r/m0s$/, "m")
    |> String.replace(~r/h0m$/, "h")
  end

  defp push_render(state) do
    state = ensure_block(state)
    state = schedule_refresh(state)

    if not is_nil(state.block_id) do
      Owl.LiveScreen.update(state.block_id, state)
    end

    state
  end

  defp ensure_block(%{block_id: nil} = state) do
    if is_pid(Process.whereis(Owl.LiveScreen)) do
      block_id = make_ref()
      Owl.LiveScreen.add_block(block_id, state: state, render: &render_block/1)
      %{state | block_id: block_id}
    else
      state
    end
  end

  defp ensure_block(state), do: state

  defp render_block(state) do
    render_dashboard(state, CrankUtils.now_ms())
  end

  defp schedule_refresh(state) do
    desired_refresh = desired_refresh(state)
    current_refresh = current_refresh(state)

    if desired_refresh == current_refresh do
      state
    else
      state
      |> cancel_refresh()
      |> start_refresh(desired_refresh)
    end
  end

  defp desired_refresh(state) do
    cond do
      Enum.any?(Map.values(state.cards), &active_card?/1) ->
        {:interval, @run_refresh_ms}

      true ->
        delays =
          state.watchers
          |> Map.values()
          |> Enum.map(&next_relative_age_delay(&1.last_update_ms, CrankUtils.now_ms()))
          |> Kernel.++(
            state.cards
            |> Map.values()
            |> Enum.flat_map(fn card ->
              Enum.map(
                Map.values(card.signals),
                &next_relative_age_delay(&1.last_seen_ms, CrankUtils.now_ms())
              )
            end)
          )
          |> Enum.reject(&is_nil/1)

        case delays do
          [] -> nil
          delays -> {:timer, Enum.min(delays)}
        end
    end
  end

  defp current_refresh(%{refresh_mode: nil}), do: nil
  defp current_refresh(%{refresh_mode: mode, refresh_value: value}), do: {mode, value}

  defp start_refresh(state, nil),
    do: %{state | refresh_mode: nil, refresh_ref: nil, refresh_value: nil}

  defp start_refresh(state, {:interval, interval_ms}) do
    {:ok, refresh_ref} = :timer.send_interval(interval_ms, :live_tick)
    %{state | refresh_mode: :interval, refresh_ref: refresh_ref, refresh_value: interval_ms}
  end

  defp start_refresh(state, {:timer, delay_ms}) do
    refresh_ref = Process.send_after(self(), :live_refresh, delay_ms)
    %{state | refresh_mode: :timer, refresh_ref: refresh_ref, refresh_value: delay_ms}
  end

  defp cancel_refresh(%{refresh_mode: :interval, refresh_ref: refresh_ref} = state) do
    :timer.cancel(refresh_ref)
    %{state | refresh_mode: nil, refresh_ref: nil, refresh_value: nil}
  end

  defp cancel_refresh(%{refresh_mode: :timer, refresh_ref: refresh_ref} = state) do
    Process.cancel_timer(refresh_ref)
    %{state | refresh_mode: nil, refresh_ref: nil, refresh_value: nil}
  end

  defp cancel_refresh(state), do: state

  defp active_card?(card) do
    is_reference(card.active_pipeline_id) or
      (is_map(card.run_state) and
         Enum.any?(Map.values(card.run_state.item_map), &(&1.state == :running)))
  end

  defp flush_live(%{block_id: nil}), do: :ok

  defp flush_live(_state) do
    Owl.LiveScreen.await_render()
    Owl.LiveScreen.flush()
  end
end
