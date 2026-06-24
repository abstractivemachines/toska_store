defmodule Toska.Commands.Config do
  @moduledoc """
  Config command for Toska.

  Handles configuration management for the Toska server.
  """

  @behaviour Toska.Commands.Command

  alias Toska.Commands.Command
  alias Toska.ConfigManager

  @impl true
  def execute(args) do
    case args do
      ["--help"] ->
        show_help()

      ["-h"] ->
        show_help()

      ["get" | rest] ->
        handle_get(rest)

      ["set" | rest] ->
        handle_set(rest)

      ["list" | rest] ->
        handle_list(rest)

      ["reset" | rest] ->
        handle_reset(rest)

      ["reload" | rest] ->
        handle_reload(rest)

      [] ->
        Command.show_error("Config command requires a subcommand")
        show_help()
        {:error, :missing_subcommand}

      [unknown | _] ->
        Command.show_error("Unknown config subcommand: #{unknown}")
        show_help()
        {:error, :unknown_subcommand}
    end
  end

  @impl true
  def show_help do
    IO.puts("""
    Manage Toska server configuration

    Usage:
      toska config <subcommand> [options]

    Subcommands:
      get <key>           Get configuration value
      set <key> <value>   Set configuration value
      list                List all configuration
      reset [key]         Reset configuration to defaults
      reload              Reload configuration from disk

    Examples:
      toska config get port
      toska config set port 8080
      toska config set host "0.0.0.0"
      toska config list
      toska config reset port
      toska config reset  # Reset all to defaults

    Available Configuration Keys:
      port        Server port (integer)
      host        Server host (string)
      env         Environment (dev|test|prod)
      log_level   Log level (debug|info|warn|error)
      data_dir    Data directory for KV store files
      aof_file    Append-only log filename (relative to data_dir)
      snapshot_file Snapshot filename (relative to data_dir)
      sync_mode   AOF sync mode (always|interval|none)
      sync_interval_ms AOF sync interval (milliseconds)
      snapshot_interval_ms Snapshot interval (milliseconds)
      ttl_check_interval_ms TTL cleanup interval (milliseconds)
      compaction_interval_ms AOF compaction interval (milliseconds)
      compaction_aof_bytes AOF size threshold for compaction (bytes)
      watch_history_limit Maximum in-memory watch events retained for replay
      replica_url Leader URL for replication follower
      replica_poll_interval_ms Follower poll interval (milliseconds)
      replica_http_timeout_ms Follower HTTP timeout (milliseconds)
      auth_token Bearer token for KV endpoints (empty disables auth)
      replication_auth_token Bearer token for replication endpoints (empty uses auth_token)
      rate_limit_per_sec Requests per second rate limit (0 disables)
      rate_limit_burst Burst capacity for rate limiting (0 disables)
    """)

    :ok
  end

  defp handle_get(args) do
    {options, remaining_args, invalid} =
      Command.parse_options(
        args,
        [
          help: :boolean
        ],
        h: :help
      )

    cond do
      options[:help] ->
        show_get_help()

      invalid != [] ->
        Command.show_error("Invalid options: #{inspect(invalid)}")
        {:error, :invalid_options}

      length(remaining_args) != 1 ->
        Command.show_error("Get command requires exactly one key")
        show_get_help()
        {:error, :invalid_args}

      true ->
        [key] = remaining_args
        get_config_value(key)
    end
  end

  defp handle_set(args) do
    {options, remaining_args, invalid} =
      Command.parse_options(
        args,
        [
          help: :boolean
        ],
        h: :help
      )

    cond do
      options[:help] ->
        show_set_help()

      invalid != [] ->
        Command.show_error("Invalid options: #{inspect(invalid)}")
        {:error, :invalid_options}

      length(remaining_args) != 2 ->
        Command.show_error("Set command requires exactly two arguments: key and value")
        show_set_help()
        {:error, :invalid_args}

      true ->
        [key, value] = remaining_args
        set_config_value(key, value)
    end
  end

  defp handle_list(args) do
    {options, _remaining_args, invalid} =
      Command.parse_options(
        args,
        [
          help: :boolean,
          json: :boolean
        ],
        h: :help,
        j: :json
      )

    cond do
      options[:help] ->
        show_list_help()

      invalid != [] ->
        Command.show_error("Invalid options: #{inspect(invalid)}")
        {:error, :invalid_options}

      true ->
        list_config(options[:json] || false)
    end
  end

  defp handle_reset(args) do
    {options, remaining_args, invalid} =
      Command.parse_options(
        args,
        [
          help: :boolean,
          confirm: :boolean
        ],
        h: :help,
        y: :confirm
      )

    cond do
      options[:help] ->
        show_reset_help()

      invalid != [] ->
        Command.show_error("Invalid options: #{inspect(invalid)}")
        {:error, :invalid_options}

      true ->
        reset_config(remaining_args, options[:confirm] || false)
    end
  end

  defp get_config_value(key) do
    case ConfigManager.get(key) do
      {:ok, value} ->
        IO.puts("#{key}: #{inspect(value)}")
        :ok

      {:error, :not_found} ->
        Command.show_error("Configuration key '#{key}' not found")
        {:error, :not_found}

      {:error, reason} ->
        Command.show_error("Failed to get configuration: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp set_config_value(key, value) do
    case ConfigManager.set(key, value) do
      :ok ->
        Command.show_success("Configuration updated: #{key} = #{inspect(value)}")
        :ok

      {:error, reason} ->
        Command.show_error("Failed to set configuration: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp list_config(json) do
    case ConfigManager.list() do
      {:ok, config} ->
        if json do
          IO.puts(Jason.encode!(config, pretty: true))
        else
          IO.puts("Current Configuration:")
          IO.puts("=====================")

          Enum.each(config, fn {key, value} ->
            IO.puts("#{key}: #{inspect(value)}")
          end)
        end

        :ok

      {:error, reason} ->
        Command.show_error("Failed to list configuration: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp reset_config(keys, confirm) do
    if keys == [] do
      reset_all_config(confirm)
    else
      reset_specific_keys(keys, confirm)
    end
  end

  defp reset_all_config(confirm) do
    if confirm or get_confirmation("This will reset ALL configuration to defaults. Continue?") do
      case ConfigManager.reset_all() do
        :ok ->
          Command.show_success("All configuration reset to defaults")
          :ok

        {:error, reason} ->
          Command.show_error("Failed to reset configuration: #{inspect(reason)}")
          {:error, reason}
      end
    else
      Command.show_info("Configuration reset cancelled")
      :ok
    end
  end

  defp reset_specific_keys(keys, _confirm) do
    results =
      Enum.map(keys, fn key ->
        case ConfigManager.reset(key) do
          :ok ->
            Command.show_success("Reset #{key} to default")
            :ok

          {:error, reason} ->
            Command.show_error("Failed to reset #{key}: #{inspect(reason)}")
            {:error, reason}
        end
      end)

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, :partial_failure}
    end
  end

  defp get_confirmation(message) do
    IO.write("#{message} (y/N): ")

    case IO.read(:line) do
      {:ok, input} ->
        trimmed_input = input |> String.trim() |> String.downcase()
        trimmed_input in ["y", "yes"]

      _ ->
        false
    end
  end

  defp show_get_help do
    IO.puts("""
    Get a configuration value

    Usage:
      toska config get <key>

    Examples:
      toska config get port
      toska config get host
    """)
  end

  defp show_set_help do
    IO.puts("""
    Set a configuration value

    Usage:
      toska config set <key> <value>

    Examples:
      toska config set port 8080
      toska config set host "0.0.0.0"
      toska config set log_level info
    """)
  end

  defp show_list_help do
    IO.puts("""
    List all configuration values

    Usage:
      toska config list [options]

    Options:
      -j, --json    Output in JSON format

    Examples:
      toska config list
      toska config list --json
    """)
  end

  defp show_reset_help do
    IO.puts("""
    Reset configuration to defaults

    Usage:
      toska config reset [key] [options]

    Options:
      -y, --confirm    Skip confirmation prompt

    Examples:
      toska config reset port
      toska config reset --confirm  # Reset all
    """)
  end

  defp handle_reload(args) do
    {options, _remaining, invalid} =
      Command.parse_options(args, [help: :boolean], h: :help)

    cond do
      options[:help] ->
        show_reload_help()

      invalid != [] ->
        Command.show_error("Invalid options: #{inspect(invalid)}")
        {:error, :invalid_options}

      true ->
        do_reload()
    end
  end

  defp do_reload do
    case ConfigManager.reload() do
      :ok ->
        Command.show_success("Configuration reloaded")
        :ok

      {:error, reason} ->
        Command.show_error("Reload failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp show_reload_help do
    IO.puts("""
    Reload configuration from disk

    Usage:
      toska config reload

    This reloads the configuration file and updates the in-memory cache.
    Changes take effect immediately for new requests.
    """)

    :ok
  end
end
