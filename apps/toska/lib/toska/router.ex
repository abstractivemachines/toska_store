defmodule Toska.Router do
  @moduledoc """
  HTTP router for Toska server using Plug.

  Defines the HTTP endpoints and handles incoming requests.
  """

  use Plug.Router

  alias Toska.ConfigManager
  alias Toska.RateLimiter

  @max_key_list_limit 1000

  # Default max body size: 10MB. Can be overridden via TOSKA_MAX_BODY_SIZE env var
  # or max_body_size config key.
  @default_max_body_size 10_485_760

  plug(Plug.Logger)
  plug(:check_content_length)

  plug(Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason,
    length: @default_max_body_size
  )

  plug(:ensure_kv_access)
  plug(:match)
  plug(:dispatch)

  # Check Content-Length header against configured max body size
  defp check_content_length(conn, _opts) do
    max_size = ConfigManager.cached_max_body_size()
    content_length = get_content_length(conn)

    if content_length != nil and content_length > max_size do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(413, Jason.encode!(%{error: "Request body too large", max_bytes: max_size}))
      |> halt()
    else
      conn
    end
  end

  defp get_content_length(conn) do
    case Plug.Conn.get_req_header(conn, "content-length") do
      [length_str | _] ->
        case Integer.parse(length_str) do
          {length, ""} -> length
          _ -> nil
        end

      _ ->
        nil
    end
  end

  # GET / - Welcome page showing server status
  get "/" do
    status = Toska.Server.status()

    response_body = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>Toska Server</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .status { padding: 20px; border-radius: 5px; margin: 20px 0; }
        .running { background-color: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
        .stopped { background-color: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }
        .error { background-color: #fff3cd; border: 1px solid #ffeaa7; color: #856404; }
        pre { background-color: #f8f9fa; padding: 15px; border-radius: 5px; overflow-x: auto; }
      </style>
    </head>
    <body>
      <h1>Toska Server</h1>
      <div class="status #{status_class(status.status)}">
        <h2>Server Status: #{String.upcase(to_string(status.status))}</h2>
        #{status_details(status)}
      </div>
      <h3>Available Endpoints:</h3>
      <ul>
        <li><a href="/">/</a> - This welcome page</li>
        <li><a href="/status">/status</a> - JSON status endpoint</li>
        <li><a href="/health">/health</a> - Health check endpoint</li>
        <li><a href="/stats">/stats</a> - KV store stats</li>
        <li>/kv/&lt;key&gt; - GET/PUT/DELETE key/value</li>
        <li>/kv/mget - POST body {"keys": ["a", "b"]}</li>
        <li>/kv/watch?prefix=&amp;since_revision=0 - Server-Sent Events key change feed</li>
        <li>/leases - POST to create, DELETE /leases/&lt;id&gt; to revoke</li>
        <li>/locks/&lt;name&gt;/acquire - POST to acquire a lease-backed lock</li>
        <li><a href="/replication/info">/replication/info</a> - Replication metadata</li>
        <li><a href="/replication/status">/replication/status</a> - Follower status</li>
        <li><a href="/replication/snapshot">/replication/snapshot</a> - Snapshot file</li>
        <li>/replication/aof?since=0 - AOF stream</li>
      </ul>
    </body>
    </html>
    """

    conn
    |> put_resp_content_type("text/html")
    |> send_resp(200, response_body)
  end

  # GET /status - JSON endpoint returning server status
  get "/status" do
    status = Toska.Server.status()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(status))
  end

  # GET /health - Health check endpoint
  get "/health" do
    status = Toska.Server.status()

    health_status =
      case status.status do
        :running -> "healthy"
        :starting -> "starting"
        _ -> "unhealthy"
      end

    response = %{
      status: health_status,
      timestamp: System.system_time(:millisecond),
      uptime: status.uptime
    }

    status_code = if health_status == "healthy", do: 200, else: 503

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status_code, Jason.encode!(response))
  end

  # GET /stats - KV store stats
  get "/stats" do
    case Toska.KVStore.stats() do
      {:ok, stats} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(stats))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # GET /metrics - Prometheus format metrics
  get "/metrics" do
    case build_prometheus_metrics() do
      {:ok, metrics} ->
        conn
        |> put_resp_content_type("text/plain; version=0.0.4; charset=utf-8")
        |> send_resp(200, metrics)

      {:error, _reason} ->
        conn
        |> put_resp_content_type("text/plain")
        |> send_resp(503, "# Metrics unavailable\n")
    end
  end

  # POST /admin/reload - Reload configuration
  post "/admin/reload" do
    case ConfigManager.reload() do
      :ok ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{ok: true, message: "Configuration reloaded"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(500, Jason.encode!(%{error: "Reload failed", reason: inspect(reason)}))
    end
  end

  # GET /replication/info - snapshot and AOF metadata
  get "/replication/info" do
    case Toska.KVStore.replication_info() do
      {:ok, info} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(info))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # GET /replication/status - follower status
  get "/replication/status" do
    case Toska.Replication.Follower.status() do
      {:ok, status} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(status))

      {:error, :not_running} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Follower not running"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "Follower unavailable", reason: inspect(reason)})
        )
    end
  end

  # GET /replication/snapshot - snapshot file for followers
  get "/replication/snapshot" do
    case Toska.KVStore.snapshot() do
      :ok ->
        case {Toska.KVStore.snapshot_path(), Toska.KVStore.replication_info()} do
          {{:ok, path}, {:ok, info}} ->
            conn
            |> put_resp_content_type("application/json")
            |> put_replication_headers(info)
            |> send_file(200, path)

          {{:ok, path}, {:error, _reason}} ->
            conn
            |> put_resp_content_type("application/json")
            |> put_resp_header("x-toska-replication-warning", "info_unavailable")
            |> send_file(200, path)

          {_, {:error, reason}} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              503,
              Jason.encode!(%{error: "Snapshot unavailable", reason: inspect(reason)})
            )

          {{:error, reason}, _} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              503,
              Jason.encode!(%{error: "Snapshot unavailable", reason: inspect(reason)})
            )
        end

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "Snapshot unavailable", reason: inspect(reason)})
        )
    end
  end

  # GET /replication/aof?since=offset&max_bytes=bytes - append-only log stream
  get "/replication/aof" do
    conn = fetch_query_params(conn)
    since_param = conn.params["since"]
    max_bytes_param = conn.params["max_bytes"]

    with {:ok, offset} <- parse_offset(since_param),
         {:ok, path} <- Toska.KVStore.aof_path() do
      size = file_size(path)
      max_bytes = parse_max_bytes(max_bytes_param)

      cond do
        offset < 0 ->
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(400, Jason.encode!(%{error: "Offset must be >= 0"}))

        offset >= size ->
          conn
          |> put_resp_content_type("application/octet-stream")
          |> put_resp_header("x-toska-aof-size", Integer.to_string(size))
          |> send_resp(204, "")

        true ->
          to_send = min(size - offset, max_bytes)

          conn
          |> put_resp_content_type("application/octet-stream")
          |> put_resp_header("x-toska-aof-size", Integer.to_string(size))
          |> put_resp_header("x-toska-aof-offset", Integer.to_string(offset))
          |> send_file(200, path, offset, to_send)
      end
    else
      {:error, :invalid_offset} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid offset"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(503, Jason.encode!(%{error: "AOF unavailable", reason: inspect(reason)}))
    end
  end

  # GET /kv/keys - list keys with cursor-based pagination
  get "/kv/keys" do
    prefix = conn.params["prefix"] || ""
    cursor = conn.params["cursor"]

    with {:ok, limit} <- parse_key_list_limit(conn.params["limit"]),
         {:ok, result} <- Toska.KVStore.list_keys_cursor(prefix, limit, cursor) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode!(result))
    else
      {:error, :invalid_limit} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid limit"}))

      {:error, :limit_too_large} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Limit too large", max: @max_key_list_limit}))

      {:error, :invalid_cursor} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid cursor"}))

      {:error, :invalid_args} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid arguments"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # GET /kv/watch?prefix=...&since_revision=... - SSE change feed
  get "/kv/watch" do
    prefix = conn.params["prefix"] || ""

    with {:ok, since_revision} <- parse_watch_revision(conn.params["since_revision"]),
         {:ok, once?} <- parse_bool_param(conn.params["once"], false),
         {:ok, timeout_ms} <- parse_watch_timeout(conn.params["timeout_ms"]),
         {:ok, watch} <- Toska.KVStore.watch(prefix, since_revision) do
      conn
      |> put_resp_content_type("text/event-stream")
      |> put_resp_header("cache-control", "no-cache")
      |> put_resp_header("x-accel-buffering", "no")
      |> put_resp_header("x-toska-revision", Integer.to_string(watch.current_revision))
      |> send_chunked(200)
      |> stream_watch(watch, once?, timeout_ms)
    else
      {:error, :invalid_watch} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid watch parameters"}))

      {:error, :history_unavailable} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(409, Jason.encode!(%{error: "Watch history unavailable"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # POST /kv/txn - compare-and-apply transaction
  post "/kv/txn" do
    compare = conn.body_params["compare"] || []
    success = conn.body_params["success"] || []
    failure = conn.body_params["failure"] || []

    case Toska.KVStore.txn(compare, success, failure) do
      {:ok, result} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(result))

      {:error, :invalid_transaction} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid transaction"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # POST /leases - create a lease with ttl_ms and optional id
  post "/leases" do
    case Toska.KVStore.create_lease(conn.body_params["ttl_ms"], %{
           id: conn.body_params["id"]
         }) do
      {:ok, lease} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(201, Jason.encode!(%{ok: true, lease: lease}))

      {:error, :invalid_lease} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid lease"}))

      {:error, :lease_exists} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(409, Jason.encode!(%{error: "Lease already exists"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # POST /leases/:id/keepalive - renew a lease to now + ttl_ms
  post "/leases/:id/keepalive" do
    case Toska.KVStore.keepalive_lease(id) do
      {:ok, lease} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{ok: true, lease: lease}))

      {:error, :invalid_lease} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid lease"}))

      {:error, :lease_not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Lease not found", id: id}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # DELETE /leases/:id - revoke a lease and remove attached keys/locks
  delete "/leases/:id" do
    case Toska.KVStore.delete_lease(id) do
      :ok ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{ok: true, id: id}))

      {:error, :invalid_lease} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid lease"}))

      {:error, :lease_not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Lease not found", id: id}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # POST /locks/:name/acquire - acquire a named lock using an active lease
  post "/locks/:name/acquire" do
    lease_id = conn.body_params["lease_id"]
    holder = conn.body_params["holder"]

    case Toska.KVStore.acquire_lock(name, lease_id, holder) do
      {:ok, lock} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{ok: true, lock: lock}))

      {:error, :invalid_lock} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid lock request"}))

      {:error, :lease_not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Lease not found", lease_id: lease_id}))

      {:error, :lock_held} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(409, Jason.encode!(%{error: "Lock held", name: name}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # POST /locks/:name/release - release a lock owned by lease_id
  post "/locks/:name/release" do
    lease_id = conn.body_params["lease_id"]

    case Toska.KVStore.release_lock(name, lease_id) do
      :ok ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{ok: true, name: name}))

      {:error, :invalid_lock} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid lock request"}))

      {:error, :lock_not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Lock not found", name: name}))

      {:error, :lock_owner_mismatch} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(409, Jason.encode!(%{error: "Lock owner mismatch", name: name}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # GET /kv/:key - fetch value
  get "/kv/:key" do
    case Toska.KVStore.get_entry(key) do
      {:ok, entry} ->
        conn
        |> put_resp_content_type("application/json")
        |> put_entry_etag(entry)
        |> send_resp(200, Jason.encode!(entry_response(entry)))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Not Found", key: key}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # PUT /kv/:key - set value with optional ttl_ms
  put "/kv/:key" do
    value = conn.body_params["value"]
    ttl_ms = conn.body_params["ttl_ms"]
    options = write_options(conn)

    cond do
      not is_binary(value) ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Value must be a string"}))

      true ->
        case Toska.KVStore.put(key, value, ttl_ms, options) do
          :ok ->
            case Toska.KVStore.get_entry(key) do
              {:ok, entry} ->
                conn
                |> put_resp_content_type("application/json")
                |> put_entry_etag(entry)
                |> send_resp(200, Jason.encode!(Map.put(entry_response(entry), :ok, true)))

              {:error, :not_found} ->
                conn
                |> put_resp_content_type("application/json")
                |> send_resp(200, Jason.encode!(%{ok: true, key: key}))
            end

          {:error, :condition_failed} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(412, Jason.encode!(%{error: "Condition failed", key: key}))

          {:error, :invalid_conditions} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(400, Jason.encode!(%{error: "Invalid conditions"}))

          {:error, :invalid_lease} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(400, Jason.encode!(%{error: "Invalid lease"}))

          {:error, :lease_not_found} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(404, Jason.encode!(%{error: "Lease not found"}))

          {:error, reason} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              503,
              Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
            )
        end
    end
  end

  # DELETE /kv/:key - remove value
  delete "/kv/:key" do
    case Toska.KVStore.delete(key, write_conditions(conn)) do
      :ok ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{ok: true, key: key}))

      {:error, :condition_failed} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(412, Jason.encode!(%{error: "Condition failed", key: key}))

      {:error, :invalid_conditions} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Invalid conditions"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          503,
          Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
        )
    end
  end

  # POST /kv/mget - fetch multiple keys
  post "/kv/mget" do
    keys = conn.body_params["keys"]

    cond do
      not is_list(keys) ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Keys must be a list"}))

      true ->
        case Toska.KVStore.mget(keys) do
          {:ok, values} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(%{values: values}))

          {:error, reason} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              503,
              Jason.encode!(%{error: "KV store unavailable", reason: inspect(reason)})
            )
        end
    end
  end

  # Catch-all for undefined routes
  match _ do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(404, Jason.encode!(%{error: "Not Found", path: conn.request_path}))
  end

  # Private helper functions

  defp status_class(:running), do: "running"
  defp status_class(:stopped), do: "stopped"
  defp status_class(_), do: "error"

  defp status_details(%{status: :running, uptime: uptime, config: config})
       when not is_nil(config) do
    """
    <p><strong>Uptime:</strong> #{format_uptime(uptime)}</p>
    <p><strong>Configuration:</strong></p>
    <pre>#{inspect(config, pretty: true)}</pre>
    """
  end

  defp status_details(%{status: :stopped}) do
    "<p>Server is currently stopped.</p>"
  end

  defp status_details(_) do
    "<p>Server status information unavailable.</p>"
  end

  defp format_uptime(nil), do: "N/A"

  defp format_uptime(uptime_ms) do
    seconds = div(uptime_ms, 1000)
    minutes = div(seconds, 60)
    hours = div(minutes, 60)

    cond do
      hours > 0 -> "#{hours}h #{rem(minutes, 60)}m #{rem(seconds, 60)}s"
      minutes > 0 -> "#{minutes}m #{rem(seconds, 60)}s"
      true -> "#{seconds}s"
    end
  end

  defp parse_offset(nil), do: {:ok, 0}
  defp parse_offset(offset) when is_integer(offset), do: {:ok, offset}

  defp parse_offset(offset) when is_binary(offset) do
    case Integer.parse(offset) do
      {value, ""} -> {:ok, value}
      _ -> {:error, :invalid_offset}
    end
  end

  defp parse_offset(_), do: {:error, :invalid_offset}

  defp parse_max_bytes(nil), do: 1024 * 1024

  defp parse_max_bytes(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> 1024 * 1024
    end
  end

  defp parse_max_bytes(_), do: 1024 * 1024

  defp parse_watch_revision(nil), do: {:ok, nil}
  defp parse_watch_revision(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp parse_watch_revision(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, :invalid_watch}
    end
  end

  defp parse_watch_revision(_value), do: {:error, :invalid_watch}

  defp parse_bool_param(nil, default), do: {:ok, default}

  defp parse_bool_param(value, _default) when is_binary(value) do
    case String.downcase(value) do
      "true" -> {:ok, true}
      "1" -> {:ok, true}
      "false" -> {:ok, false}
      "0" -> {:ok, false}
      _ -> {:error, :invalid_watch}
    end
  end

  defp parse_bool_param(_value, _default), do: {:error, :invalid_watch}

  defp parse_watch_timeout(nil), do: {:ok, :infinity}

  defp parse_watch_timeout(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, :invalid_watch}
    end
  end

  defp parse_watch_timeout(_), do: {:error, :invalid_watch}

  defp stream_watch(conn, watch, once?, timeout_ms) do
    case chunk(conn, ": connected\n\n") do
      {:ok, conn} ->
        case chunk_watch_events(conn, watch.events) do
          {:ok, conn} ->
            if once? do
              Toska.KVStore.unwatch(watch.ref)
              conn
            else
              stream_watch_loop(conn, watch.ref, timeout_ms)
            end

          {:error, _reason} ->
            Toska.KVStore.unwatch(watch.ref)
            conn
        end

      {:error, _reason} ->
        Toska.KVStore.unwatch(watch.ref)
        conn
    end
  end

  defp chunk_watch_events(conn, events) do
    Enum.reduce_while(events, {:ok, conn}, fn event, {:ok, conn} ->
      case chunk(conn, sse_event(event)) do
        {:ok, conn} -> {:cont, {:ok, conn}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stream_watch_loop(conn, ref, :infinity) do
    receive do
      {Toska.KVStore, :watch_event, ^ref, event} ->
        case chunk(conn, sse_event(event)) do
          {:ok, conn} ->
            stream_watch_loop(conn, ref, :infinity)

          {:error, _reason} ->
            Toska.KVStore.unwatch(ref)
            conn
        end
    end
  end

  defp stream_watch_loop(conn, ref, timeout_ms) do
    receive do
      {Toska.KVStore, :watch_event, ^ref, event} ->
        case chunk(conn, sse_event(event)) do
          {:ok, conn} ->
            stream_watch_loop(conn, ref, timeout_ms)

          {:error, _reason} ->
            Toska.KVStore.unwatch(ref)
            conn
        end
    after
      timeout_ms ->
        Toska.KVStore.unwatch(ref)
        conn
    end
  end

  defp sse_event(event) do
    [
      "id: ",
      Integer.to_string(event.revision),
      "\n",
      "event: ",
      event.op,
      "\n",
      "data: ",
      Jason.encode!(event),
      "\n\n"
    ]
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %File.Stat{size: size}} -> size
      _ -> 0
    end
  end

  defp put_replication_headers(conn, info) do
    conn
    |> maybe_put_header(
      "x-toska-snapshot-checksum",
      Map.get(info, "snapshot_checksum") || Map.get(info, :snapshot_checksum)
    )
    |> maybe_put_header(
      "x-toska-snapshot-version",
      Map.get(info, "snapshot_version") || Map.get(info, :snapshot_version)
    )
    |> maybe_put_header(
      "x-toska-aof-version",
      Map.get(info, "aof_version") || Map.get(info, :aof_version)
    )
  end

  defp maybe_put_header(conn, _key, nil), do: conn

  defp maybe_put_header(conn, key, value) do
    put_resp_header(conn, key, to_string(value))
  end

  defp ensure_kv_access(conn, _opts) do
    if kv_path?(conn.request_path) do
      conn
      |> ensure_auth()
      |> ensure_rate_limit()
      |> ensure_read_only()
    else
      conn
    end
  end

  defp ensure_auth(%Plug.Conn{halted: true} = conn), do: conn

  defp ensure_auth(conn) do
    token = auth_token(conn.request_path)

    if token == "" do
      conn
    else
      header = get_req_header(conn, "authorization") |> List.first()
      alt_header = get_req_header(conn, "x-toska-token") |> List.first()

      if token_match?(token, header) or token_match?(token, alt_header) do
        conn
      else
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(401, Jason.encode!(%{error: "Unauthorized"}))
        |> halt()
      end
    end
  end

  defp ensure_rate_limit(%Plug.Conn{halted: true} = conn), do: conn

  defp ensure_rate_limit(conn) do
    {per_sec, burst} = rate_limit_config()

    if RateLimiter.allowed?(client_key(conn), per_sec, burst) do
      conn
    else
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(429, Jason.encode!(%{error: "Rate limit exceeded"}))
      |> halt()
    end
  end

  defp ensure_read_only(%Plug.Conn{halted: true} = conn), do: conn

  defp ensure_read_only(conn) do
    if follower_mode?() and write_request?(conn) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(403, Jason.encode!(%{error: "Read-only follower"}))
      |> halt()
    else
      conn
    end
  end

  defp kv_path?(path) do
    String.starts_with?(path, "/kv") or
      String.starts_with?(path, "/leases") or
      String.starts_with?(path, "/locks") or
      path == "/stats" or
      String.starts_with?(path, "/replication")
  end

  defp write_request?(conn) do
    method = conn.method
    path = conn.request_path

    (method in ["PUT", "DELETE"] and String.starts_with?(path, "/kv/")) or
      (method == "POST" and path == "/kv/txn") or
      (method in ["POST", "DELETE"] and String.starts_with?(path, "/leases")) or
      (method == "POST" and String.starts_with?(path, "/locks"))
  end

  defp follower_mode? do
    ConfigManager.cached_follower_mode?()
  end

  defp auth_token(path) do
    if String.starts_with?(path, "/replication") do
      ConfigManager.cached_replication_auth_token()
    else
      ConfigManager.cached_auth_token()
    end
  end

  defp token_match?(_token, nil), do: false

  defp token_match?(token, header) when is_binary(header) do
    header == token or header == "Bearer #{token}"
  end

  defp token_match?(_token, _header), do: false

  defp rate_limit_config do
    ConfigManager.cached_rate_limit()
  end

  defp entry_response(entry) do
    %{
      key: entry.key,
      value: entry.value,
      metadata: entry_metadata(entry)
    }
  end

  defp entry_metadata(entry) do
    %{
      version: entry.version,
      created_at: entry.created_at,
      updated_at: entry.updated_at,
      expires_at: entry.expires_at,
      lease_id: Map.get(entry, :lease_id)
    }
  end

  defp put_entry_etag(conn, entry) do
    put_resp_header(conn, "etag", etag(entry.version))
  end

  defp etag(version), do: ~s("#{version}")

  defp write_conditions(conn) do
    %{
      if_version:
        first_present([
          conn.body_params["if_version"],
          conn.params["if_version"],
          if_match_version(conn)
        ]),
      if_absent:
        first_present([
          conn.body_params["if_absent"],
          conn.params["if_absent"],
          if_none_match_absent(conn)
        ]),
      if_present:
        first_present([
          conn.body_params["if_present"],
          conn.params["if_present"]
        ])
    }
  end

  defp write_options(conn) do
    conn
    |> write_conditions()
    |> Map.put(
      :lease_id,
      first_present([
        conn.body_params["lease_id"],
        conn.params["lease_id"]
      ])
    )
  end

  defp first_present(values) do
    Enum.find(values, &(not is_nil(&1)))
  end

  defp if_match_version(conn) do
    conn
    |> get_req_header("if-match")
    |> List.first()
    |> parse_etag_version()
  end

  defp if_none_match_absent(conn) do
    case get_req_header(conn, "if-none-match") |> List.first() do
      "*" -> true
      _ -> nil
    end
  end

  defp parse_etag_version(nil), do: nil

  defp parse_etag_version(value) do
    value
    |> String.trim()
    |> String.trim_leading("W/")
    |> String.trim("\"")
  end

  defp parse_key_list_limit(nil), do: {:ok, 100}

  defp parse_key_list_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> validate_key_list_limit(int)
      _ -> {:error, :invalid_limit}
    end
  end

  defp parse_key_list_limit(value) when is_integer(value), do: validate_key_list_limit(value)
  defp parse_key_list_limit(_), do: {:error, :invalid_limit}

  defp validate_key_list_limit(value) when value < 0, do: {:error, :invalid_limit}

  defp validate_key_list_limit(value) when value > @max_key_list_limit,
    do: {:error, :limit_too_large}

  defp validate_key_list_limit(value), do: {:ok, value}

  defp client_key(conn) do
    case conn.remote_ip do
      {_, _, _, _} = ip -> to_string(:inet.ntoa(ip))
      {_, _, _, _, _, _, _, _} = ip -> to_string(:inet.ntoa(ip))
      _ -> "unknown"
    end
  end

  defp build_prometheus_metrics do
    with {:ok, stats} <- Toska.KVStore.stats() do
      server_status = Toska.Server.status()
      uptime_seconds = (server_status.uptime || 0) / 1000
      env = server_status[:config][:env] || "unknown"

      base_metrics = """
      # HELP toska_keys_total Total number of keys in the store
      # TYPE toska_keys_total gauge
      toska_keys_total #{stats.keys}

      # HELP toska_memory_words ETS memory usage in words
      # TYPE toska_memory_words gauge
      toska_memory_words #{stats.memory_words}

      # HELP toska_aof_bytes Current AOF file size in bytes
      # TYPE toska_aof_bytes gauge
      toska_aof_bytes #{stats.aof_bytes}

      # HELP toska_snapshot_bytes Current snapshot file size in bytes
      # TYPE toska_snapshot_bytes gauge
      toska_snapshot_bytes #{stats.snapshot_bytes}

      # HELP toska_uptime_seconds Server uptime in seconds
      # TYPE toska_uptime_seconds counter
      toska_uptime_seconds #{uptime_seconds}

      # HELP toska_server_info Server information
      # TYPE toska_server_info gauge
      toska_server_info{version="#{Toska.version()}",env="#{env}"} 1

      # HELP toska_last_snapshot_timestamp_seconds Unix timestamp of last snapshot
      # TYPE toska_last_snapshot_timestamp_seconds gauge
      toska_last_snapshot_timestamp_seconds #{(stats.last_snapshot_at || 0) / 1000}

      # HELP toska_last_sync_timestamp_seconds Unix timestamp of last AOF sync
      # TYPE toska_last_sync_timestamp_seconds gauge
      toska_last_sync_timestamp_seconds #{(stats.last_sync_at || 0) / 1000}
      """

      # Add replication metrics if in follower mode
      metrics =
        case Toska.Replication.Follower.status() do
          {:ok, follower} ->
            lag = replication_lag(follower.last_poll_at)

            base_metrics <>
              """

              # HELP toska_replication_offset Current replication offset
              # TYPE toska_replication_offset gauge
              toska_replication_offset #{follower.offset}

              # HELP toska_replication_lag_seconds Time since last poll
              # TYPE toska_replication_lag_seconds gauge
              toska_replication_lag_seconds #{lag}
              """

          _ ->
            base_metrics
        end

      {:ok, metrics}
    else
      _ -> {:error, :unavailable}
    end
  end

  defp replication_lag(nil), do: -1

  defp replication_lag(last_poll_at) do
    (System.system_time(:millisecond) - last_poll_at) / 1000
  end
end
