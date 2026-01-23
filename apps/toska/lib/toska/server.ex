defmodule Toska.Server do
  @moduledoc """
  Server management module for Toska.

  This module provides the foundation for server operations and will eventually
  become a full GenServer implementation for the Toska server process.
  """

  use GenServer
  require Logger

  alias Toska.ConfigManager
  alias Toska.KVStore
  alias Toska.NodeControl
  alias Toska.RateLimiter
  alias Toska.Replication.Follower

  @name __MODULE__
  @http_server_name :"#{__MODULE__}.HTTPServer"

  # Client API

  @doc """
  Start the Toska server with the given options.
  """
  def start(opts \\ []) do
    host = Keyword.get(opts, :host, "localhost")
    port = Keyword.get(opts, :port, 4000)
    env = Keyword.get(opts, :env, "dev")
    daemon = Keyword.get(opts, :daemon, false)

    case GenServer.start_link(
           __MODULE__,
           %{
             host: host,
             port: port,
             env: env,
             daemon: daemon,
             started_at: System.system_time(:millisecond)
           },
           name: @name
         ) do
      {:ok, pid} ->
        Logger.info("Toska server started on #{host}:#{port} (env: #{env})")

        case NodeControl.ensure_server_node() do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning("Distributed control disabled: #{inspect(reason)}")
        end

        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:error, {:already_started, pid}}

      {:error, reason} ->
        Logger.error("Failed to start Toska server: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Stop the Toska server.
  """
  def stop(opts \\ []) do
    force = Keyword.get(opts, :force, false)

    case GenServer.whereis(@name) do
      nil ->
        {:error, :not_running}

      pid ->
        result =
          if force do
            try do
              Process.exit(pid, :kill)
              Logger.info("Toska server force stopped")
              :ok
            rescue
              ArgumentError -> {:error, :not_running}
            end
          else
            try do
              GenServer.stop(pid, :normal)
              Logger.info("Toska server stopped gracefully")
              :ok
            catch
              :exit, _ -> {:error, :not_running}
            end
          end

        NodeControl.clear_runtime()
        result
    end
  end

  @doc """
  Get the current status of the Toska server.
  """
  def status do
    case GenServer.whereis(@name) do
      nil ->
        %{
          status: :stopped,
          uptime: nil,
          config: nil
        }

      _pid ->
        try do
          GenServer.call(@name, :get_status)
        catch
          :exit, {:noproc, _} ->
            %{
              status: :error,
              uptime: nil,
              config: nil
            }
        end
    end
  end

  @doc """
  Get server configuration.
  """
  def get_config do
    case GenServer.whereis(@name) do
      nil ->
        {:error, :not_running}

      _pid ->
        GenServer.call(@name, :get_config)
    end
  end

  @doc """
  Update server configuration.
  """
  def update_config(new_config) do
    case GenServer.whereis(@name) do
      nil ->
        {:error, :not_running}

      _pid ->
        GenServer.call(@name, {:update_config, new_config})
    end
  end

  # GenServer Callbacks

  @impl true
  def init(state) do
    Logger.info("Initializing Toska server with config: #{inspect(state)}")

    # Initialize rate limiter ETS table once at startup
    RateLimiter.init()

    case KVStore.start_link() do
      {:ok, store_pid} ->
        start_replication_follower()
        start_http_and_set_state(state, store_pid)

      {:error, {:already_started, store_pid}} ->
        start_replication_follower()
        start_http_and_set_state(state, store_pid)

      {:error, reason} ->
        Logger.error("Failed to start KV store: #{inspect(reason)}")
        {:stop, {:kv_store_failed, reason}}
    end
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    uptime =
      case state.started_at do
        nil -> nil
        started_at -> System.system_time(:millisecond) - started_at
      end

    status_info = %{
      status: state.status,
      uptime: uptime,
      config: Map.take(state, [:host, :port, :env, :daemon]),
      pid: inspect(self()),
      node: to_string(Node.self())
    }

    {:reply, status_info, state}
  end

  @impl true
  def handle_call(:get_config, _from, state) do
    config = Map.take(state, [:host, :port, :env, :daemon])
    {:reply, {:ok, config}, state}
  end

  @impl true
  def handle_call({:update_config, new_config}, _from, state) do
    updated_state = Map.merge(state, new_config)
    Logger.info("Server configuration updated: #{inspect(new_config)}")
    {:reply, :ok, updated_state}
  end

  @impl true
  def handle_info(:server_ready, state) do
    Logger.info("Toska server is ready and accepting connections")
    {:noreply, Map.put(state, :status, :running)}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("Received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Toska server terminating: #{inspect(reason)}")

    # Stop the HTTP server if it's running
    if Map.has_key?(state, :bandit_pid) and state.bandit_pid do
      stop_http_server()
    end

    case GenServer.whereis(KVStore) do
      nil -> :ok
      _pid -> KVStore.stop()
    end

    NodeControl.clear_runtime()
    :ok
  end

  # Private helper functions

  defp start_http_server(state) do
    base_options = [
      plug: Toska.Router,
      port: state.port,
      ip: parse_host(state.host),
      # Thousand Island (underlying server) options for better concurrency
      thousand_island_options: [
        # Number of acceptor processes - more acceptors = better connection acceptance under load
        num_acceptors: System.schedulers_online() * 2,
        # Graceful shutdown timeout
        shutdown_timeout: 30_000
      ],
      # HTTP options
      http_options: [
        # Compress responses over 1KB
        compress: true
      ]
    ]

    bandit_options =
      case build_tls_options() do
        nil ->
          base_options

        tls_opts ->
          Logger.info("TLS enabled for HTTP server")

          base_options
          |> Keyword.put(:scheme, :https)
          |> Keyword.update!(:thousand_island_options, fn opts ->
            Keyword.merge(opts, tls_opts)
          end)
      end

    case Bandit.start_link(bandit_options) do
      {:ok, pid} ->
        # Register the pid with our own name for management
        Process.register(pid, @http_server_name)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_tls_options do
    config = Toska.ConfigManager.tls_config()

    if config.enabled and config.cert_file != "" and config.key_file != "" do
      # Validate files exist
      cert_path = Path.expand(config.cert_file)
      key_path = Path.expand(config.key_file)

      unless File.exists?(cert_path) do
        Logger.error("TLS certificate file not found: #{cert_path}")
        raise "TLS certificate file not found: #{cert_path}"
      end

      unless File.exists?(key_path) do
        Logger.error("TLS key file not found: #{key_path}")
        raise "TLS key file not found: #{key_path}"
      end

      base_tls = [
        certfile: String.to_charlist(cert_path),
        keyfile: String.to_charlist(key_path),
        # Modern TLS settings
        versions: [:"tlsv1.3", :"tlsv1.2"]
      ]

      tls_opts =
        if config.verify_client and config.ca_cert_file != "" do
          ca_path = Path.expand(config.ca_cert_file)

          unless File.exists?(ca_path) do
            Logger.error("TLS CA certificate file not found: #{ca_path}")
            raise "TLS CA certificate file not found: #{ca_path}"
          end

          Logger.info("mTLS enabled - client certificate verification required")

          # mTLS: require and verify client certificates
          base_tls ++
            [
              cacertfile: String.to_charlist(ca_path),
              verify: :verify_peer,
              fail_if_no_peer_cert: true
            ]
        else
          base_tls
        end

      [transport_options: tls_opts]
    else
      nil
    end
  end

  defp start_http_and_set_state(state, store_pid) do
    case start_http_server(state) do
      {:ok, bandit_pid} ->
        Logger.info("HTTP server started successfully on #{state.host}:#{state.port}")
        Process.send_after(self(), :server_ready, 500)

        {:ok,
         Map.merge(state, %{status: :starting, bandit_pid: bandit_pid, store_pid: store_pid})}

      {:error, reason} ->
        Logger.error("Failed to start HTTP server: #{inspect(reason)}")
        {:stop, {:http_server_failed, reason}}
    end
  end

  defp start_replication_follower do
    case replication_options() do
      nil ->
        :ok

      opts ->
        case Follower.start_link(opts) do
          {:ok, _pid} ->
            :ok

          {:error, {:already_started, _pid}} ->
            :ok

          {:error, reason} ->
            Logger.warning("Replication follower failed to start: #{inspect(reason)}")
        end
    end
  end

  defp replication_options do
    config =
      case GenServer.whereis(ConfigManager) do
        nil ->
          %{}

        _pid ->
          case ConfigManager.list() do
            {:ok, stored} -> stored
            _ -> %{}
          end
      end

    url = System.get_env("TOSKA_REPLICA_URL") || config["replica_url"]

    if is_binary(url) and url != "" do
      [
        leader_url: url,
        poll_interval_ms:
          parse_int(
            System.get_env("TOSKA_REPLICA_POLL_MS"),
            config["replica_poll_interval_ms"],
            1000
          ),
        http_timeout_ms:
          parse_int(
            System.get_env("TOSKA_REPLICA_HTTP_TIMEOUT_MS"),
            config["replica_http_timeout_ms"],
            5000
          )
      ]
    else
      nil
    end
  end

  defp parse_int(nil, nil, default), do: default
  defp parse_int(nil, value, default), do: parse_int(value, default)
  defp parse_int(value, _default, default), do: parse_int(value, default)

  defp parse_int(value, _default) when is_integer(value) and value > 0, do: value

  defp parse_int(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> default
    end
  end

  defp parse_int(_, default), do: default

  defp stop_http_server do
    case Process.whereis(@http_server_name) do
      nil ->
        Logger.debug("HTTP server already stopped")
        :ok

      pid ->
        Process.unregister(@http_server_name)
        GenServer.stop(pid, :normal)
        Logger.info("HTTP server stopped")
        :ok
    end
  end

  defp parse_host("localhost"), do: {127, 0, 0, 1}
  defp parse_host("127.0.0.1"), do: {127, 0, 0, 1}
  defp parse_host("0.0.0.0"), do: {0, 0, 0, 0}

  defp parse_host(host) when is_binary(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> ip
      # Default to localhost on parse error
      {:error, _} -> {127, 0, 0, 1}
    end
  end

  defp parse_host(_), do: {127, 0, 0, 1}
end
