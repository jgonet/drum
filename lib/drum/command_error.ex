defmodule Drum.CommandError do
  defexception [:exit_code, :cmd]

  @impl true
  def message(%{exit_code: code, cmd: cmd}) do
    "command #{inspect(cmd)} failed with exit code #{code}"
  end
end
