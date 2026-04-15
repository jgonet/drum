defmodule Crank.Output.LiveTest do
  use ExUnit.Case, async: true

  alias Crank.Output.Live

  test "format_relative_age/2 buckets time without churn" do
    assert Live.format_relative_age(nil, 0) == "never"
    assert Live.format_relative_age(0, 10_000) == "now"
    assert Live.format_relative_age(0, 30_000) == "30s ago"
    assert Live.format_relative_age(0, 90_000) == "1m30s ago"
    assert Live.format_relative_age(0, 300_000) == "5m ago"
    assert Live.format_relative_age(0, 720_000) == "12m ago"
  end

  test "summarize_changed_paths/2 keeps single paths exact and groups larger updates" do
    cwd = File.cwd!()
    src_root = Path.join(cwd, "src")

    assert Live.summarize_changed_paths([Path.join(src_root, "foo.ts")], [src_root]) == "foo.ts"

    changed = [
      Path.join(cwd, "lib/vm/a.c"),
      Path.join(cwd, "lib/vm/b.c"),
      Path.join(cwd, "c_src/c.h")
    ]

    assert Live.summarize_changed_paths(changed, [Path.join(cwd, "lib"), Path.join(cwd, "c_src")]) ==
             "vm/ (2), c_src/ (1)"
  end

  test "render_dashboard/2 shows watcher rows, pending signals, and graceful stop notes" do
    home = System.user_home!()
    watch_ref = make_ref()
    card_key = make_ref()
    pipeline_id = make_ref()
    step_id = make_ref()

    watch_signal =
      {:watch, %{watch: watch_ref, changed: [Path.join(home, "dev/popcorn/src/foo.ts")]}}

    run_state = %{
      cmd_buffers: %{},
      cmd_seq: 0,
      failed_step: "build",
      group_children: %{},
      item_map: %{
        step_id => %{
          end_ms: 1_100,
          id: step_id,
          name: "build",
          reason: {:stopped, :graceful},
          start_ms: 900,
          state: :failed,
          stop_reason: "watch#1",
          type: :step
        }
      },
      items: [step_id],
      pipeline_id: pipeline_id,
      pipeline_start_ms: 900,
      skipped_steps: 0,
      status: :failed,
      successful_steps: 0
    }

    state = %{
      Live.init([])
      | watchers_order: [watch_ref],
        watchers: %{
          watch_ref => %{
            generated_index: 1,
            last_changed: [Path.join(home, "dev/popcorn/src/foo.ts")],
            last_update_ms: 1_100,
            patterns: [Path.join(home, "dev/popcorn/src/**/*.{js,ts}")],
            roots: [Path.join(home, "dev/popcorn/src")],
            watch_ref: watch_ref
          }
        },
        cards_order: [card_key],
        cards: %{
          card_key => %{
            active_pipeline_id: nil,
            active_run_n: nil,
            card_key: card_key,
            generated_index: nil,
            header_state: :restarting,
            label: "Build JS",
            last_activity_ms: 1_100,
            restart_reason: %{fallback_label: "watch#1", mode: :graceful, signal: watch_signal},
            run_count: 1,
            run_state: run_state,
            signals: %{
              {:watch, watch_ref} => %{
                fallback_label: "watch#1",
                last_seen_ms: 1_100,
                pending: true,
                signal: watch_signal
              }
            }
          }
        }
    }

    rendered =
      state
      |> Live.render_dashboard(1_100)
      |> Owl.Data.to_chardata()
      |> IO.iodata_to_binary()
      |> strip_ansi()

    assert rendered =~ "╭─ Watchers"
    assert rendered =~ "watch#1 · last now"
    assert rendered =~ "  watching: ~/dev/popcorn/src/**/*.{js,ts}"
    assert rendered =~ "  last: foo.ts"
    assert rendered =~ "╭─ Build JS · restarting"
    assert rendered =~ "watch#1 · pending · 1 run · last now"
    assert rendered =~ "✕ build"
    assert rendered =~ "(stopped by watch#1)"
  end

  test "render_dashboard/2 rolls managed tmp watch paths onto separate lines" do
    watch_ref = make_ref()
    tmp_dir = Path.join(Crank.TmpDir.tmp_dirs_root(), "0b9d18e7")

    state = %{
      Live.init([])
      | watchers_order: [watch_ref],
        watchers: %{
          watch_ref => %{
            generated_index: 2,
            last_changed: [Path.join(tmp_dir, "lib/vm/runtime.c")],
            last_update_ms: 1_100,
            patterns: [
              Path.join(tmp_dir, "lib/vm/**/*.{c,h}"),
              Path.join(tmp_dir, "c_src/**/*.{c,h}")
            ],
            roots: [Path.join(tmp_dir, "lib/vm"), Path.join(tmp_dir, "c_src")],
            watch_ref: watch_ref
          }
        }
    }

    rendered =
      state
      |> Live.render_dashboard(1_100)
      |> Owl.Data.to_chardata()
      |> IO.iodata_to_binary()
      |> strip_ansi()

    assert rendered =~ "watch#2 · last now"
    assert rendered =~ "  watching: lib/vm/**/*.{c,h} in tmp-0b9d18e7,"
    assert rendered =~ "            c_src/**/*.{c,h} in tmp-0b9d18e7"
    assert rendered =~ "  last: runtime.c"
  end

  test "render_dashboard/2 keeps cards in registration order" do
    first_key = make_ref()
    second_key = make_ref()

    state = %{
      Live.init([])
      | cards_order: [first_key, second_key],
        cards: %{
          first_key => %{
            active_pipeline_id: nil,
            active_run_n: nil,
            card_key: first_key,
            generated_index: nil,
            header_state: nil,
            label: "Build VM",
            last_activity_ms: 1_000,
            restart_reason: nil,
            run_count: 1,
            run_state: nil,
            signals: %{
              vm: %{
                fallback_label: "vm",
                last_seen_ms: 1_000,
                pending: false,
                signal: {:vm_compiled, %{label: "vm compiled"}}
              }
            }
          },
          second_key => %{
            active_pipeline_id: make_ref(),
            active_run_n: 1,
            card_key: second_key,
            generated_index: nil,
            header_state: nil,
            label: "Build JS",
            last_activity_ms: 2_000,
            restart_reason: nil,
            run_count: 1,
            run_state: nil,
            signals: %{
              js: %{
                fallback_label: "js",
                last_seen_ms: 2_000,
                pending: true,
                signal: {:watch, %{watch: make_ref(), changed: []}}
              }
            }
          }
        }
    }

    rendered =
      state
      |> Live.render_dashboard(2_000)
      |> Owl.Data.to_chardata()
      |> IO.iodata_to_binary()
      |> strip_ansi()

    {build_vm_index, _length} = :binary.match(rendered, "╭─ Build VM")
    {build_js_index, _length} = :binary.match(rendered, "╭─ Build JS")

    assert build_vm_index < build_js_index
  end

  defp strip_ansi(output) do
    Regex.replace(~r/\e\[[\d;]*m/, output, "")
  end
end
