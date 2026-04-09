defmodule Crank.MixProject do
  use Mix.Project

  def project do
    [
      app: :crank,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Crank.Application, []},
      extra_applications: [:logger, :erlexec]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:erlexec, "~> 2.2"},
      {:owl, "~> 0.13"},
      {:dotenvy, "~> 0.8", optional: true}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
