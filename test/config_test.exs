defmodule Crank.ConfigTest do
  use ExUnit.Case, async: true

  alias Crank.Config

  def id_resolver(raw), do: raw

  defp flags_config(flags),
    do: %{flags: flags, positional: [], env: [], resolve: &__MODULE__.id_resolver/1}

  @config_ok %{
    flags: [
      name: [:string],
      verbose: [boolean: false],
      jobs: [integer: nil],
      format: [enum: :plain, values: [:plain, :json]]
    ],
    positional: [],
    env: [],
    resolve: &__MODULE__.id_resolver/1
  }

  describe "to_context/2: flags" do
    test "parses all flag types" do
      argv = ["--name", "alice", "--verbose", "--jobs", "4", "--format", "json"]
      ctx = Config.to_context!({argv, %{}}, @config_ok)
      assert %{name: "alice", verbose: true, jobs: 4, format: :json} = ctx
    end

    test "fills optional flag defaults" do
      ctx = Config.to_context!({["--name", "alice"], %{}}, @config_ok)
      assert %{verbose: false, jobs: nil, format: :plain} = ctx
    end

    test "rejects missing required flags" do
      assert {:error, {:missing_required, [:name]}} = Config.to_context({[], %{}}, @config_ok)
    end

    test "rejects unknown flags" do
      argv = ["--name", "alice", "--nope", "x"]
      assert {:error, {:bad_flags, _}} = Config.to_context({argv, %{}}, @config_ok)
    end

    test "rejects invalid enum flags" do
      argv = ["--name", "alice", "--format", "xml"]
      assert {:error, {:bad_value, :format}} = Config.to_context({argv, %{}}, @config_ok)
    end

    test "expands short aliases" do
      config = flags_config(jobs: [integer: 1, short: :j])
      ctx = Config.to_context!({["-j", "8"], %{}}, config)
      assert 8 = ctx.jobs
    end

    test "normalizes kebab-case flag names to snake_case" do
      config = flags_config("dry-run": [boolean: false])
      ctx = Config.to_context!({["--dry-run"], %{}}, config)
      assert true = ctx.dry_run
    end

    test "normalizes kebab-case enum flag names to snake_case" do
      config = flags_config("output-format": [enum: :plain, values: [:plain, :json]])
      ctx = Config.to_context!({["--output-format", "json"], %{}}, config)
      assert :json = ctx.output_format
    end
  end

  describe "to_context/2: resolve" do
    test "keeps parsed inputs in ctx.raw" do
      ctx = Config.to_context!({["--name", "alice"], %{}}, @config_ok)
      assert "alice" = ctx.raw.name
      assert not ctx.raw.verbose
    end

    test "resolver can transform raw values" do
      upcase_resolver = fn raw -> Map.put(raw, :upcased, String.upcase(raw.name)) end

      config = %{flags: [name: [:string]], positional: [], env: [], resolve: upcase_resolver}
      ctx = Config.to_context!({["--name", "alice"], %{}}, config)
      assert "ALICE" = ctx.upcased
    end

    test "wraps resolver exceptions" do
      raising_resolver = fn _ -> raise "boom" end
      config = %{flags: [name: [:string]], positional: [], env: [], resolve: raising_resolver}
      result = Config.to_context({["--name", "x"], %{}}, config)
      assert {:error, {:bad_resolver, %RuntimeError{message: "boom"}}} = result
    end

    test "rejects non-map resolver results" do
      non_map_resolver = fn _ -> :bad end
      config = %{flags: [name: [:string]], positional: [], env: [], resolve: non_map_resolver}
      assert {:error, {:bad_resolver, :bad}} = Config.to_context({["--name", "x"], %{}}, config)
    end

    test "returns only raw values without a resolver" do
      config = %{flags: [verbose: [boolean: false]], positional: [], env: []}
      assert {:ok, %{raw: %{verbose: false}}} = Config.to_context({[], %{}}, config)
    end

    test "resolver can combine flags and env values" do
      resolver = fn raw -> Map.put(raw, :source, raw.source || raw[:FALLBACK_SOURCE]) end

      config = %{
        flags: [source: [string: nil]],
        positional: [],
        env: [FALLBACK_SOURCE: [string: nil]],
        resolve: resolver
      }

      result = Config.to_context({[], %{"FALLBACK_SOURCE" => "fallback"}}, config)
      assert {:ok, %{source: "fallback"}} = result
    end
  end

  test "to_context/2: name collisions" do
    config = %{
      flags: [name: [:string]],
      positional: [name: [:string]],
      env: [],
      resolve: &__MODULE__.id_resolver/1
    }

    assert {:error, {:key_collision, [:name]}} = Config.to_context({[], %{}}, config)
  end

  @positional_mode %{
    flags: [],
    positional: [mode: [:enum, values: [:debug, :release]]],
    env: [],
    resolve: &__MODULE__.id_resolver/1
  }

  describe "to_context/2: positional" do
    test "parses required enum values" do
      result = Config.to_context({["debug"], %{}}, @positional_mode)
      assert {:ok, %{mode: :debug}} = result
    end

    test "rejects missing required values" do
      assert {:error, _} = Config.to_context({[], %{}}, @positional_mode)
    end

    test "rejects extra values" do
      external = {["debug", "release"], %{}}

      assert {:error, {:extra_positional, ["release"]}} =
               Config.to_context(external, @positional_mode)
    end
  end

  describe "to_context/2: env" do
    test "parses optional env vars" do
      config = %{
        flags: [],
        positional: [],
        env: [MY_VAR: [string: nil]],
        resolve: &__MODULE__.id_resolver/1
      }

      result = Config.to_context({[], %{"MY_VAR" => "hello"}}, config)
      assert {:ok, ctx} = result
      assert "hello" = ctx[:MY_VAR]
    end

    test "rejects missing required env vars" do
      config = %{
        flags: [],
        positional: [],
        env: [MY_VAR: [:string]],
        resolve: &__MODULE__.id_resolver/1
      }

      assert {:error, _} = Config.to_context({[], %{}}, config)
    end
  end
end
