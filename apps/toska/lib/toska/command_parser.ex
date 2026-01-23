defmodule Toska.CommandParser do
  @moduledoc """
  Command parser for Toska.

  Handles parsing of command-line arguments and routing to appropriate command handlers.
  """

  alias Toska.Commands

  @doc """
  Parse command line arguments and execute the appropriate command.

  ## Examples

      iex> Toska.CommandParser.parse(["start", "--port", "8080"])
      :ok

      iex> Toska.CommandParser.parse(["--help"])
      :ok
  """
  def parse(args) do
    case args do
      [] ->
        show_help()

      ["--help"] ->
        show_help()

      ["-h"] ->
        show_help()

      ["--version"] ->
        show_version()

      ["start" | rest] ->
        Commands.Start.execute(rest)

      ["stop" | rest] ->
        Commands.Stop.execute(rest)

      ["status" | rest] ->
        Commands.Status.execute(rest)

      ["config" | rest] ->
        Commands.Config.execute(rest)

      ["replicate" | rest] ->
        Commands.Replicate.execute(rest)

      ["backup" | rest] ->
        Commands.Backup.execute(rest)

      ["restore" | rest] ->
        Commands.Restore.execute(rest)

      [command | _] ->
        IO.puts("Unknown command: #{command}")
        show_help()
        {:error, :unknown_command}
    end
  end

  @doc """
  Display help information for the CLI.
  """
  def show_help do
    IO.puts("""
    Toska - Command Line Interface for ToskaStore

    Usage:
      toska [command] [options]

    Commands:
      start     Start the Toska server
      stop      Stop the Toska server
      status    Check server status
      config    Manage configuration
      replicate Start replication follower
      backup    Create backup of KV store
      restore   Restore from backup

    Global Options:
      -h, --help    Show this help message
      --version     Show version information

    Examples:
      toska start --port 8080
      toska stop
      toska status
      toska config get port
      toska config set port 9000
      toska config reload
      toska replicate --leader http://localhost:4000
      toska backup /var/backups/toska/
      toska restore /var/backups/toska/

    For command-specific help, use:
      toska [command] --help
    """)

    :ok
  end

  def show_version do
    IO.puts("toska #{Toska.version()}")
    IO.puts("Copyright 2025 Abstractive Machines LLC")
    :ok
  end
end
