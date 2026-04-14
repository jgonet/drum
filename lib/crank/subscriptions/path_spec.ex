defmodule Crank.Subscriptions.PathSpec do
  defstruct [:glob, :path]

  def normalize_paths!(path) when is_binary(path), do: normalize_paths!([path])

  def normalize_paths!(paths) when is_list(paths) do
    if not Enum.all?(paths, &is_binary/1) do
      raise ArgumentError, "expected paths to be binaries, got: #{inspect(paths)}"
    end

    Enum.map(paths, &new/1)
  end

  def normalize_paths!(paths) do
    raise ArgumentError, "expected a path or list of paths, got: #{inspect(paths)}"
  end

  def new(path) when is_binary(path) do
    expanded_path = Path.expand(path)
    path = pattern_root_path(expanded_path)
    glob = GlobEx.compile!(expanded_path)
    %__MODULE__{glob: glob, path: path}
  end

  def path(spec), do: spec.path

  def valid?(path) when is_binary(path) do
    expanded_path = Path.expand(path)
    has_trailing_slash = String.ends_with?(path, "/")
    is_dir = has_trailing_slash or File.dir?(expanded_path)
    is_glob = glob_path?(path)

    not is_dir or is_glob
  end

  def match?(spec, changed_path) when is_binary(changed_path) do
    expanded_path = Path.expand(changed_path)
    GlobEx.match?(spec.glob, expanded_path)
  end

  defp glob_path?(path) do
    path
    |> Path.split()
    |> Enum.any?(&glob_segment?/1)
  end

  defp pattern_root_path(path) when is_binary(path) do
    if glob_path?(path) do
      literal_segments =
        path
        |> Path.split()
        |> Enum.take_while(&(not glob_segment?(&1)))

      Path.join(literal_segments)
    else
      path
    end
  end

  defp glob_segment?("\\" <> rest), do: glob_segment_after_escape(rest)
  defp glob_segment?("*" <> _rest), do: true
  defp glob_segment?("?" <> _rest), do: true
  defp glob_segment?("[" <> _rest), do: true
  defp glob_segment?("{" <> _rest), do: true
  defp glob_segment?(<<_character::utf8, rest::binary>>), do: glob_segment?(rest)
  defp glob_segment?(<<>>), do: false

  defp glob_segment_after_escape(<<_character::utf8, rest::binary>>), do: glob_segment?(rest)
  defp glob_segment_after_escape(""), do: false
end
