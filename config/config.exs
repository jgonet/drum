import Config

if config_env() == :test do
  config :crank, :output, {Crank.Output.Test, []}
end
