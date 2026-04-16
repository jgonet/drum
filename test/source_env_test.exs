defmodule SourceEnvTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  defp write_env(tmp_dir, name, content) do
    path = Path.join(tmp_dir, name)
    File.write!(path, content)
    path
  end

  test "merges two maps, later wins" do
    result = Drum.source_env([%{"A" => "1", "B" => "2"}, %{"B" => "3", "C" => "4"}])
    assert result == %{"A" => "1", "B" => "3", "C" => "4"}
  end

  test "loads a .env file", %{tmp_dir: tmp_dir} do
    path = write_env(tmp_dir, "test_load.env", "X=hello\nY=world\n")
    result = Drum.source_env([path])
    assert result["X"] == "hello"
    assert result["Y"] == "world"
  end

  test "merges file then map, map wins", %{tmp_dir: tmp_dir} do
    path = write_env(tmp_dir, "test_merge.env", "KEY=from_file\n")
    result = Drum.source_env([path, %{"KEY" => "from_map"}])
    assert result["KEY"] == "from_map"
  end

  test "merges map then file, file wins", %{tmp_dir: tmp_dir} do
    path = write_env(tmp_dir, "test_merge2.env", "KEY=from_file\n")
    result = Drum.source_env([%{"KEY" => "from_map"}, path])
    assert result["KEY"] == "from_file"
  end

  test "raises on missing file path" do
    assert_raise RuntimeError, ~r/file not found/, fn ->
      Drum.source_env(["/nonexistent/path/.env"])
    end
  end
end
