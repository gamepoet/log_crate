defmodule LogCrate.Mixfile do
  use Mix.Project

  def project do
    [
      app: :log_crate,
      version: "0.0.1",
      elixir: "~> 1.0",
      name: "LogCrate",
      source_url: "https://github.com/gamepoet/log_crate",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps,
    ]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [
      {:earmark,  ">= 0.0.0", only: :dev},
      {:ex_doc,   "~> 0.8", only: :dev},
      {:uuid,     "~> 1.0", only: :test},
    ]
  end
end
