defmodule Crank.Utils do
  def now_ms(), do: System.monotonic_time(:millisecond)

  def now_ms_wall, do: System.system_time(:millisecond)

  def map_ok(items, f) do
    results =
      Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
        case f.(item) do
          {:ok, mapped} -> {:cont, {:ok, [mapped | acc]}}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case results do
      {:ok, mapped} -> {:ok, Enum.reverse(mapped)}
      {:error, _} = err -> err
    end
  end

  def reduce_ok(items, acc, f) do
    Enum.reduce_while(items, {:ok, acc}, fn item, {:ok, inner_acc} ->
      case f.(item, inner_acc) do
        {:ok, new_acc} -> {:cont, {:ok, new_acc}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  def resolve_cd(nil, _ctx, _run_opts), do: nil
  def resolve_cd(path, _ctx, _run_opts) when is_binary(path), do: path
  def resolve_cd(key, ctx, _run_opts) when is_atom(key), do: Map.get(ctx, key)
  def resolve_cd(f, ctx, run_opts) when is_function(f, 2), do: f.(ctx, run_opts)

  def eval_condition(nil, _ctx), do: true
  def eval_condition(true, _ctx), do: true
  def eval_condition(false, _ctx), do: false
  def eval_condition(key, ctx) when is_atom(key), do: !!Map.get(ctx, key)
  def eval_condition(f, ctx) when is_function(f, 1), do: !!f.(ctx)

  def kw_key_collisions(kws) when is_list(kws) do
    all_keys = Enum.flat_map(kws, &Keyword.keys/1)
    duplicates = Enum.uniq(all_keys -- Enum.uniq(all_keys))
    duplicates
  end
end
