defmodule Toska.MixProject do
  use Mix.Project

  def project do
    [
      app: :toska,
      version: "0.9.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      test_coverage: [summary: [threshold: 80]],
      deps: deps(),
      escript: escript()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :inets, :ssl],
      mod: {Toska.Application, []}
    ]
  end

  # Configuration for building an escript
  def escript do
    [
      main_module: Toska.CLI,
      name: "toska"
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:bandit, "~> 1.0"},
      {:plug, "~> 1.15"},
      {:benchee, "~> 1.2", only: :dev, runtime: false}
    ]
  end
end
