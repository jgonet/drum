defmodule Drum.Config do
  alias Drum.Config.Decl
  alias Drum.Utils

  defstruct flags: [],
            positional: [],
            env: [],
            resolve: nil

  def to_context!({argv, env}, %{} = raw_config) do
    {:ok, ctx} = to_context({argv, env}, raw_config)
    ctx
  end

  def to_context({argv, env}, %{} = raw_config) do
    with {:ok, flag_decls} <- Decl.parse(raw_config.flags),
         {:ok, positional_decls} <- Decl.parse(raw_config.positional),
         {:ok, env_decls} <- Decl.parse(raw_config.env),
         :ok <- check_no_key_collisions([flag_decls, positional_decls, env_decls]),
         {:ok, flag_values, raw_positional} <- parse_argv(argv, flag_decls),
         {:ok, positional_values} <- parse_positional(raw_positional, positional_decls),
         {:ok, env_values} <- parse_env(env, env_decls),
         raw = Map.new(flag_values ++ positional_values ++ env_values),
         {:ok, %{} = resolved} <- resolve_context(raw, Map.get(raw_config, :resolve)) do
      {:ok, Map.put(resolved, :raw, raw)}
    end
  end

  defp check_no_key_collisions(decls) do
    case Utils.kw_key_collisions(decls) do
      [] -> :ok
      duplicates -> {:error, {:key_collision, duplicates}}
    end
  end

  defp parse_argv(argv, flag_decls) do
    switches =
      Keyword.new(flag_decls, fn
        {name, %Decl{type: {:enum, _}}} -> {name, :string}
        {name, %Decl{type: t}} -> {name, t}
      end)

    aliases = for {name, %Decl{short: short}} <- flag_decls, is_atom(short), do: {short, name}
    {parsed, rest, unexpected} = OptionParser.parse(argv, strict: switches, aliases: aliases)

    with {:ok, parsed} <- parse_enum_flags(parsed, flag_decls),
         parsed <- add_flag_defaults(parsed, flag_decls),
         :ok <- check_required_flags(parsed, flag_decls),
         :ok <- check_no_unexpected(unexpected) do
      {:ok, parsed, rest}
    end
  end

  defguardp is_enum(type) when is_tuple(type) and tuple_size(type) == 2 and elem(type, 0) == :enum

  defp parse_enum_flags(flags, flags_decls) do
    enums = Keyword.filter(flags_decls, fn {_name, decl} -> is_enum(decl.type) end)

    Utils.reduce_ok(enums, flags, fn {name, %Decl{type: {:enum, values}}}, acc ->
      case Keyword.fetch(acc, name) do
        {:ok, value} -> coerce_enum(acc, name, value, values)
        :error -> {:ok, acc}
      end
    end)
  end

  defp coerce_enum(acc, name, value, values) do
    case Enum.find(values, &(to_string(&1) == value)) do
      nil -> {:error, {:bad_value, name}}
      atom -> {:ok, Keyword.put(acc, name, atom)}
    end
  end

  defp add_flag_defaults(flags, flag_decls) do
    Enum.reduce(flag_decls, flags, fn {name, decl}, acc ->
      optional_key_missing = not Keyword.has_key?(acc, name) and not decl.required

      if optional_key_missing do
        Keyword.put(acc, name, decl.default)
      else
        acc
      end
    end)
  end

  defp check_required_flags(flags, flag_decls) do
    required_keys =
      Keyword.filter(flag_decls, fn {_name, decl} -> decl.required end)
      |> Keyword.keys()
      |> MapSet.new()

    keys = flags |> Keyword.keys() |> MapSet.new()
    missing = MapSet.difference(required_keys, keys)

    case MapSet.size(missing) do
      0 -> :ok
      _ -> {:error, {:missing_required, MapSet.to_list(missing)}}
    end
  end

  defp check_no_unexpected([]), do: :ok
  defp check_no_unexpected(unexpected), do: {:error, {:bad_flags, unexpected}}

  defp parse_positional(raw, positional_decls) when length(raw) > length(positional_decls) do
    extra = Enum.drop(raw, length(positional_decls))
    {:error, {:extra_positional, extra}}
  end

  defp parse_positional(raw, positional_decls) do
    {covered, remaining} = Enum.split(positional_decls, length(raw))

    with {:ok, parsed} <- parse_positional_values(Enum.zip(raw, covered)),
         :ok <- check_required_positional(remaining) do
      defaults = Enum.map(remaining, fn {name, decl} -> {name, decl.default} end)
      {:ok, parsed ++ defaults}
    end
  end

  defp parse_env(env, env_decls) do
    Utils.map_ok(env_decls, fn {name, decl} ->
      case Map.get(env, Atom.to_string(name)) do
        nil when decl.required -> {:error, {:missing_required, [name]}}
        nil -> {:ok, {name, decl.default}}
        value -> with {:ok, v} <- coerce(value, decl), do: {:ok, {name, v}}
      end
    end)
  end

  defp parse_positional_values(pairs) do
    Utils.map_ok(pairs, fn {value, {name, decl}} ->
      with {:ok, v} <- coerce(value, decl), do: {:ok, {name, v}}
    end)
  end

  defp resolve_context(%{} = raw, resolve_f) when is_function(resolve_f, 1) do
    case resolve_f.(raw) do
      context when is_map(context) -> {:ok, context}
      bad_context -> {:error, {:bad_resolver, bad_context}}
    end
  rescue
    e -> {:error, {:bad_resolver, e}}
  end

  defp resolve_context(_raw, nil), do: {:ok, %{}}

  defp resolve_context(_raw, resolve_f), do: {:error, {:bad_resolver, resolve_f}}

  defp check_required_positional(remaining) do
    required? = fn {_, d} -> d.required end

    case Enum.find(remaining, required?) do
      nil -> :ok
      {name, _} -> {:error, {:missing_required, [name]}}
    end
  end

  defp coerce(value, %Decl{type: {:enum, values}}) do
    case Enum.find(values, &(to_string(&1) == value)) do
      nil -> {:error, {:bad_value, value}}
      atom -> {:ok, atom}
    end
  end

  defp coerce(value, %Decl{type: :string}), do: {:ok, value}

  defp coerce(value, %Decl{type: :integer}) do
    {:ok, String.to_integer(value)}
  rescue
    ArgumentError -> {:error, {:bad_value, value}}
  end
end
