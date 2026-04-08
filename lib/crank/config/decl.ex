defmodule Crank.Config.Decl do
  @moduledoc false
  alias Crank.Utils
  defstruct type: nil, default: nil, required: false, short: nil, doc: nil

  @type_keys ~w(string boolean integer enum)a

  def parse(decls) when is_list(decls), do: Utils.map_ok(decls, &parse_single/1)
  def parse(decls), do: {:error, {:bad_format, decls}}

  defp parse_single({name, kw}) when is_atom(name) and is_list(kw) do
    case parse_type(kw) do
      {:ok, {type, default, required}} ->
        parsed = %__MODULE__{
          type: type,
          default: default,
          required: required,
          short: Keyword.get(kw, :short),
          doc: Keyword.get(kw, :doc)
        }

        {:ok, {name, parsed}}

      {:error, {:bad_decl, inner}} ->
        {:error, {:bad_decl, {name, inner}}}
    end
  end

  defp parse_single(decl), do: {:error, {:bad_decl, decl}}

  # {type, default, required}
  # {:enum, {default, values}, required}
  defp parse_type([:enum | _] = decl) do
    case Keyword.fetch(decl, :values) do
      {:ok, values} -> {:ok, {{:enum, values}, nil, true}}
      :error -> {:error, {:bad_decl, decl}}
    end
  end

  defp parse_type([{:enum, default} | _] = decl) do
    with {:ok, values} <- Keyword.fetch(decl, :values),
         true <- default in values do
      {:ok, {{:enum, values}, default, false}}
    else
      _ -> {:error, {:bad_decl, decl}}
    end
  end

  defp parse_type([type | _]) when type in @type_keys, do: {:ok, {type, nil, true}}

  defp parse_type([{type, default} | _] = decl) when type in @type_keys do
    if valid_default?(type, default) do
      {:ok, {type, default, false}}
    else
      {:error, {:bad_decl, decl}}
    end
  end

  defp parse_type(decl), do: {:error, {:bad_decl, decl}}

  defp valid_default?(:string, v), do: is_nil(v) or is_binary(v)
  defp valid_default?(:integer, v), do: is_nil(v) or is_integer(v)
  defp valid_default?(:boolean, v), do: is_boolean(v)
end
