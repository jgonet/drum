import Config

if config_env() == :test do
  config :crank, :file_system_opts, latency: 0, no_defer: true
end
