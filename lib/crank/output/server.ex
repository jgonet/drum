defmodule Crank.Output.Server do
  use GenServer

  def emit(event) do
    GenServer.cast(__MODULE__, event)
  end

  def start_link(args) do
    {output_mod, init_args} = Keyword.fetch!(args, :mod)
    GenServer.start_link(__MODULE__, {output_mod, init_args}, name: __MODULE__)
  end

  @impl true
  def init({output_mod, init_args}) do
    {mod, args} = resolve(output_mod, init_args)
    {:ok, {mod, mod.init(args)}}
  end

  defp resolve(Crank.Output.Live, args) do
    if IO.ANSI.enabled?() do
      {Crank.Output.Live, args}
    else
      {Crank.Output.Plain, args}
    end
  end

  defp resolve(mod, args), do: {mod, args}

  @impl true
  def handle_cast(event, {mod, mod_state}) do
    new_mod_state = mod.handle_event(event, mod_state)
    {:noreply, {mod, new_mod_state}}
  end

  @impl true
  def handle_info(event, {mod, mod_state}) do
    new_mod_state = mod.handle_event(event, mod_state)
    {:noreply, {mod, new_mod_state}}
  end

  @impl true
  def terminate(_reason, {mod, mod_state}) do
    mod.handle_event(:terminate, mod_state)
  end
end
