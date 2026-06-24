defmodule Toska.ConfigManager do
  @moduledoc """
  Configuration manager for Toska.

  Handles reading, writing, and managing configuration for the Toska CLI and server.
  Configuration is stored in a simple key-value format and persisted to disk.

  Hot-path configuration values (auth_token, replication_auth_token, rate limits, replica_url) are cached
  in persistent_term for lock-free reads on every HTTP request.
  """

  use GenServer
  require Logger

  @name __MODULE__
  @config_file "toska_config.json"

  # Keys cached in persistent_term for hot-path access (avoid GenServer calls per request)
  @cached_keys [
    "auth_token",
    "replication_auth_token",
    "rate_limit_per_sec",
    "rate_limit_burst",
    "replica_url",
    "max_body_size"
  ]
  @default_sync_interval_ms 1000
  @default_snapshot_interval_ms 60_000
  @default_ttl_check_interval_ms 1000
  @default_watch_history_limit 10_000

  # Client API

  @doc """
  Start the ConfigManager GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  @doc """
  Get a configuration value by key.
  """
  def get(key) when is_binary(key) do
    GenServer.call(@name, {:get, key})
  end

  def get(key) when is_atom(key) do
    get(Atom.to_string(key))
  end

  @doc """
  Set a configuration value.
  """
  def set(key, value) when is_binary(key) do
    GenServer.call(@name, {:set, key, value})
  end

  def set(key, value) when is_atom(key) do
    set(Atom.to_string(key), value)
  end

  @doc """
  List all configuration values.
  """
  def list do
    GenServer.call(@name, :list)
  end

  @doc """
  Reset a specific configuration key to its default value.
  """
  def reset(key) when is_binary(key) do
    GenServer.call(@name, {:reset, key})
  end

  def reset(key) when is_atom(key) do
    reset(Atom.to_string(key))
  end

  @doc """
  Reset all configuration to default values.
  """
  def reset_all do
    GenServer.call(@name, :reset_all)
  end

  @doc """
  Reload configuration from disk.

  Re-reads the configuration file and updates the in-memory cache.
  Changes take effect immediately for new requests.
  """
  def reload do
    GenServer.call(@name, :reload)
  end

  @doc """
  Get the path to the configuration file.
  """
  def config_file_path do
    Path.join([config_dir(), @config_file])
  end

  def config_dir do
    case System.get_env("TOSKA_CONFIG_DIR") do
      nil -> Path.join([System.user_home(), ".toska"])
      "" -> Path.join([System.user_home(), ".toska"])
      dir -> dir
    end
  end

  # Hot-path accessors - read from persistent_term cache (lock-free)
  # These avoid GenServer calls on every HTTP request

  @doc """
  Get the cached auth token. Returns empty string if not set.
  Environment variable TOSKA_AUTH_TOKEN takes precedence.
  """
  def cached_auth_token do
    case System.get_env("TOSKA_AUTH_TOKEN") do
      nil -> :persistent_term.get({__MODULE__, :auth_token}, "")
      "" -> :persistent_term.get({__MODULE__, :auth_token}, "")
      token -> token
    end
  end

  @doc """
  Get the cached replication auth token. Falls back to auth_token when unset.
  Environment variable TOSKA_REPLICATION_AUTH_TOKEN takes precedence.
  """
  def cached_replication_auth_token do
    token =
      case System.get_env("TOSKA_REPLICATION_AUTH_TOKEN") do
        nil -> :persistent_term.get({__MODULE__, :replication_auth_token}, "")
        "" -> :persistent_term.get({__MODULE__, :replication_auth_token}, "")
        value -> value
      end

    if token == "" do
      cached_auth_token()
    else
      token
    end
  end

  @doc """
  Get the cached rate limit config. Returns {per_sec, burst}.
  Environment variables TOSKA_RATE_LIMIT_PER_SEC and TOSKA_RATE_LIMIT_BURST take precedence.
  """
  def cached_rate_limit do
    env_per = System.get_env("TOSKA_RATE_LIMIT_PER_SEC")
    env_burst = System.get_env("TOSKA_RATE_LIMIT_BURST")

    per_sec =
      case env_per do
        nil -> :persistent_term.get({__MODULE__, :rate_limit_per_sec}, 0)
        "" -> :persistent_term.get({__MODULE__, :rate_limit_per_sec}, 0)
        val -> parse_int_or_default(val, 0)
      end

    burst =
      case env_burst do
        nil -> :persistent_term.get({__MODULE__, :rate_limit_burst}, 0)
        "" -> :persistent_term.get({__MODULE__, :rate_limit_burst}, 0)
        val -> parse_int_or_default(val, 0)
      end

    {per_sec, burst}
  end

  @doc """
  Check if running in follower/replica mode. Returns boolean.
  Environment variable TOSKA_REPLICA_URL takes precedence.
  """
  def cached_follower_mode? do
    case System.get_env("TOSKA_REPLICA_URL") do
      nil ->
        url = :persistent_term.get({__MODULE__, :replica_url}, "")
        is_binary(url) and url != ""

      "" ->
        url = :persistent_term.get({__MODULE__, :replica_url}, "")
        is_binary(url) and url != ""

      _url ->
        true
    end
  end

  @doc """
  Get the cached max body size for HTTP requests.
  Environment variable TOSKA_MAX_BODY_SIZE takes precedence.
  Default: 10MB (10_485_760 bytes)
  """
  def cached_max_body_size do
    case System.get_env("TOSKA_MAX_BODY_SIZE") do
      nil -> :persistent_term.get({__MODULE__, :max_body_size}, 10_485_760)
      "" -> :persistent_term.get({__MODULE__, :max_body_size}, 10_485_760)
      val -> parse_int_or_default(val, 10_485_760)
    end
  end

  @doc """
  Get the TLS configuration for the server.
  Environment variables take precedence over config file.
  Returns a map with :enabled, :cert_file, :key_file, :ca_cert_file, :verify_client
  """
  def tls_config do
    case GenServer.whereis(@name) do
      nil ->
        tls_config_from_env(%{})

      _pid ->
        case list() do
          {:ok, config} -> tls_config_from_env(config)
          _ -> tls_config_from_env(%{})
        end
    end
  end

  defp tls_config_from_env(config) do
    %{
      enabled: env_bool("TOSKA_TLS_ENABLED", config["tls_enabled"]),
      cert_file: System.get_env("TOSKA_TLS_CERT_FILE") || config["tls_cert_file"] || "",
      key_file: System.get_env("TOSKA_TLS_KEY_FILE") || config["tls_key_file"] || "",
      ca_cert_file: System.get_env("TOSKA_TLS_CA_CERT_FILE") || config["tls_ca_cert_file"] || "",
      verify_client: env_bool("TOSKA_TLS_VERIFY_CLIENT", config["tls_verify_client"])
    }
  end

  defp env_bool(key, config_value) do
    case System.get_env(key) do
      "true" -> true
      "1" -> true
      "false" -> false
      "0" -> false
      nil -> config_value == true
      "" -> config_value == true
      _ -> false
    end
  end

  defp parse_int_or_default(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {int, ""} when int >= 0 -> int
      _ -> default
    end
  end

  defp parse_int_or_default(val, _default) when is_integer(val), do: val
  defp parse_int_or_default(_, default), do: default

  # GenServer Callbacks

  @impl true
  def init(_opts) do
    config_path = config_file_path()
    config_dir = Path.dirname(config_path)

    # Ensure config directory exists
    File.mkdir_p!(config_dir)

    # Load existing config or create default
    config = load_config(config_path)

    # Populate persistent_term cache for hot-path values
    update_cache(config)

    Logger.info("ConfigManager started with config file: #{config_path}")

    {:ok, %{config: config, file_path: config_path}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case Map.get(state.config, key) do
      nil ->
        {:reply, {:error, :not_found}, state}

      value ->
        {:reply, {:ok, parse_value(value)}, state}
    end
  end

  @impl true
  def handle_call({:set, key, value}, _from, state) do
    # Validate the key and value
    case validate_config_pair(key, value) do
      {:ok, validated_value} ->
        new_config = Map.put(state.config, key, validated_value)
        new_state = %{state | config: new_config}

        case save_config(new_state.config, state.file_path) do
          :ok ->
            # Update cache if this is a hot-path key
            if key in @cached_keys, do: update_cache(new_config)
            Logger.info("Configuration updated: #{key} = #{inspect(validated_value)}")
            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error("Failed to save configuration: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:list, _from, state) do
    parsed_config = Enum.into(state.config, %{}, fn {k, v} -> {k, parse_value(v)} end)
    {:reply, {:ok, parsed_config}, state}
  end

  @impl true
  def handle_call({:reset, key}, _from, state) do
    case Map.get(default_config(), key) do
      nil ->
        {:reply, {:error, :unknown_key}, state}

      default_value ->
        new_config = Map.put(state.config, key, default_value)
        new_state = %{state | config: new_config}

        case save_config(new_state.config, state.file_path) do
          :ok ->
            # Update cache if this is a hot-path key
            if key in @cached_keys, do: update_cache(new_config)
            Logger.info("Configuration key '#{key}' reset to default: #{inspect(default_value)}")
            {:reply, :ok, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_call(:reset_all, _from, state) do
    new_state = %{state | config: default_config()}

    case save_config(new_state.config, state.file_path) do
      :ok ->
        # Update cache with new defaults
        update_cache(new_state.config)
        Logger.info("All configuration reset to defaults")
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:reload, _from, state) do
    new_config = load_config(state.file_path)
    update_cache(new_config)
    Logger.info("Configuration reloaded from #{state.file_path}")
    {:reply, :ok, %{state | config: new_config}}
  end

  # Private Functions

  defp update_cache(config) do
    # Cache hot-path values in persistent_term for lock-free reads
    :persistent_term.put({__MODULE__, :auth_token}, config["auth_token"] || "")

    :persistent_term.put(
      {__MODULE__, :replication_auth_token},
      config["replication_auth_token"] || ""
    )

    :persistent_term.put(
      {__MODULE__, :rate_limit_per_sec},
      parse_int_or_default(config["rate_limit_per_sec"], 0)
    )

    :persistent_term.put(
      {__MODULE__, :rate_limit_burst},
      parse_int_or_default(config["rate_limit_burst"], 0)
    )

    :persistent_term.put({__MODULE__, :replica_url}, config["replica_url"] || "")

    :persistent_term.put(
      {__MODULE__, :max_body_size},
      parse_int_or_default(config["max_body_size"], 10_485_760)
    )
  end

  defp load_config(file_path) do
    default = default_config()

    case File.read(file_path) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, config} ->
            # Merge with defaults to ensure all keys are present
            Map.merge(default, config)

          {:error, reason} ->
            Logger.warning("Failed to parse config file, using defaults: #{inspect(reason)}")
            default
        end

      {:error, :enoent} ->
        Logger.info("Config file not found, creating with defaults")
        save_config(default, file_path)
        default

      {:error, reason} ->
        Logger.warning("Failed to read config file, using defaults: #{inspect(reason)}")
        default
    end
  end

  defp save_config(config, file_path) do
    case Jason.encode(config, pretty: true) do
      {:ok, json} ->
        case File.write(file_path, json) do
          :ok ->
            case File.chmod(file_path, 0o600) do
              :ok ->
                :ok

              {:error, reason} ->
                Logger.warning("Failed to chmod config file: #{inspect(reason)}")
                :ok
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_config_pair(key, value) do
    case key do
      "port" ->
        validate_port(value)

      "host" ->
        validate_host(value)

      "env" ->
        validate_env(value)

      "log_level" ->
        validate_log_level(value)

      "data_dir" ->
        validate_path(value)

      "aof_file" ->
        validate_path(value)

      "snapshot_file" ->
        validate_path(value)

      "sync_mode" ->
        validate_sync_mode(value)

      "sync_interval_ms" ->
        validate_positive_int(value)

      "snapshot_interval_ms" ->
        validate_positive_int(value)

      "ttl_check_interval_ms" ->
        validate_positive_int(value)

      "compaction_interval_ms" ->
        validate_positive_int(value)

      "compaction_aof_bytes" ->
        validate_positive_int(value)

      "watch_history_limit" ->
        validate_positive_int(value)

      "replica_url" ->
        validate_optional_string(value)

      "replica_poll_interval_ms" ->
        validate_positive_int(value)

      "replica_http_timeout_ms" ->
        validate_positive_int(value)

      "auth_token" ->
        validate_optional_string(value)

      "replication_auth_token" ->
        validate_optional_string(value)

      "rate_limit_per_sec" ->
        validate_nonnegative_int(value)

      "rate_limit_burst" ->
        validate_nonnegative_int(value)

      _ ->
        # Allow unknown keys for extensibility
        {:ok, value}
    end
  end

  defp validate_port(value) when is_integer(value) and value > 0 and value <= 65535 do
    {:ok, value}
  end

  defp validate_port(value) when is_binary(value) do
    case Integer.parse(value) do
      {port, ""} when port > 0 and port <= 65535 ->
        {:ok, port}

      _ ->
        {:error, "Port must be an integer between 1 and 65535"}
    end
  end

  defp validate_port(_), do: {:error, "Port must be an integer between 1 and 65535"}

  defp validate_host(value) when is_binary(value) and byte_size(value) > 0 do
    {:ok, value}
  end

  defp validate_host(_), do: {:error, "Host must be a non-empty string"}

  defp validate_env(value) when value in ["dev", "test", "prod"] do
    {:ok, value}
  end

  defp validate_env(_), do: {:error, "Environment must be one of: dev, test, prod"}

  defp validate_log_level(value) when value in ["debug", "info", "warn", "error"] do
    {:ok, value}
  end

  defp validate_log_level(_), do: {:error, "Log level must be one of: debug, info, warn, error"}

  defp validate_sync_mode(value) when value in ["always", "interval", "none"] do
    {:ok, value}
  end

  defp validate_sync_mode(_), do: {:error, "Sync mode must be one of: always, interval, none"}

  defp validate_positive_int(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp validate_positive_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> {:ok, int}
      _ -> {:error, "Value must be a positive integer"}
    end
  end

  defp validate_positive_int(_), do: {:error, "Value must be a positive integer"}

  defp validate_path(value) when is_binary(value) and byte_size(value) > 0 do
    {:ok, value}
  end

  defp validate_path(_), do: {:error, "Value must be a non-empty string"}

  defp validate_optional_string(value) when is_binary(value), do: {:ok, value}
  defp validate_optional_string(nil), do: {:ok, nil}
  defp validate_optional_string(_), do: {:error, "Value must be a string or empty"}

  defp validate_nonnegative_int(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp validate_nonnegative_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, "Value must be a non-negative integer"}
    end
  end

  defp validate_nonnegative_int(_), do: {:error, "Value must be a non-negative integer"}

  defp parse_value(value) when is_binary(value) do
    # Try to parse as integer first
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> value
    end
  end

  defp parse_value(value), do: value

  defp default_config do
    base_dir = config_dir()

    %{
      "port" => 4000,
      "host" => "localhost",
      "env" => "dev",
      "log_level" => "info",
      "data_dir" => Path.join([base_dir, "data"]),
      "aof_file" => "toska.aof",
      "snapshot_file" => "toska_snapshot.json",
      "sync_mode" => "interval",
      "sync_interval_ms" => @default_sync_interval_ms,
      "snapshot_interval_ms" => @default_snapshot_interval_ms,
      "ttl_check_interval_ms" => @default_ttl_check_interval_ms,
      "compaction_interval_ms" => 300_000,
      "compaction_aof_bytes" => 10_485_760,
      "watch_history_limit" => @default_watch_history_limit,
      "replica_url" => "",
      "replica_poll_interval_ms" => 1000,
      "replica_http_timeout_ms" => 5000,
      "auth_token" => "",
      "replication_auth_token" => "",
      "rate_limit_per_sec" => 0,
      "rate_limit_burst" => 0,
      # TLS configuration
      "tls_enabled" => false,
      "tls_cert_file" => "",
      "tls_key_file" => "",
      "tls_ca_cert_file" => "",
      "tls_verify_client" => false,
      # Request limits
      "max_body_size" => 10_485_760
    }
  end
end
