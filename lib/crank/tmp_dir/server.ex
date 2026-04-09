defmodule Crank.TmpDir.Server do
  use GenServer, restart: :temporary

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args.name)
  end

  def create(pipeline_id, mode) do
    GenServer.call(Crank.Registry.tmp_dir(pipeline_id), {:create, mode})
  end

  def cleanup(pipeline_id) do
    GenServer.call(Crank.Registry.tmp_dir(pipeline_id), :cleanup)
  end

  @impl true
  def init(_args) do
    {:ok, %{dirs: []}}
  end

  @impl true
  def handle_call({:create, mode}, _from, state) do
    path = Path.join(System.tmp_dir!(), "crank-#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(path)
    meta = JSON.encode!(%{mode: Atom.to_string(mode), pid: System.pid()})
    File.write!(Path.join(path, ".crank"), meta)
    {:reply, path, %{state | dirs: [{path, mode} | state.dirs]}}
  end

  @impl true
  def handle_call(:cleanup, _from, state) do
    do_cleanup(state.dirs)
    {:reply, :ok, %{state | dirs: []}}
  end

  @impl true
  def terminate(_reason, state) do
    do_cleanup(state.dirs)
    :ok
  end

  defp do_cleanup(dirs) do
    for {path, :transient} <- dirs, do: File.rm_rf!(path)
  end
end
