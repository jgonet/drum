defmodule Crank.Output.Utils do
  @moduledoc false

  def format_duration(ms) when ms < 1000, do: "#{ms}ms"

  def format_duration(ms) when ms < 60_000 do
    s = Float.round(ms / 1000, 2)

    s_str =
      if s == trunc(s),
        do: "#{trunc(s)}",
        else: :erlang.float_to_binary(s, [{:decimals, 2}, :compact])

    "#{s_str}s"
  end

  def format_duration(ms) when ms < 3_600_000 do
    total_s = div(ms, 1000)
    "#{div(total_s, 60)}m#{rem(total_s, 60)}s"
  end

  def format_duration(ms) do
    total_m = div(ms, 60_000)
    "#{div(total_m, 60)}h#{rem(total_m, 60)}m"
  end

  def format_parts(parts), do: parts |> Enum.reject(&is_nil/1) |> Enum.join(", ")

  def pluralize(1, word), do: word
  def pluralize(_, word), do: "#{word}s"

  def pad2(n) when n < 10, do: "0#{n}"
  def pad2(n), do: "#{n}"
end
