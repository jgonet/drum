defmodule Crank.TmpDir do
  def sweep do
    System.tmp_dir!()
    |> Path.join("crank-*")
    |> Path.wildcard()
    |> Enum.filter(&File.dir?/1)
    |> Enum.each(&maybe_delete/1)
  end

  defp maybe_delete(dir) do
    meta_path = Path.join(dir, ".crank")

    with {:ok, contents} <- File.read(meta_path),
         {:ok, %{"mode" => "transient", "pid" => pid}} <- JSON.decode(contents),
         false <- pid_alive?(pid) do
      File.rm_rf!(dir)
    end
  end

  defp pid_alive?(pid_str) do
    case System.cmd("kill", ["-0", pid_str], stderr_to_stdout: true) do
      {_, 0} -> true
      _ -> false
    end
  end
end
