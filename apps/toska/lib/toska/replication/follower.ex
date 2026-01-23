defmodule Toska.Replication.Follower do
  @moduledoc """
  Simple follower that applies snapshots and tails the AOF stream.
  """

  use GenServer
  require Logger

  alias Toska.ConfigManager
  alias Toska.KVStore

  @default_poll_interval_ms 1000
  @default_http_timeout_ms 5000

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def status do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :status)
    end
  end

  @impl true
  def init(opts) do
    leader_url = Keyword.get(opts, :leader_url)

    if is_binary(leader_url) and leader_url != "" do
      ensure_http()
      ensure_store()

      state = %{
        leader_url: String.trim_trailing(leader_url, "/"),
        poll_interval_ms: Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms),
        http_timeout_ms: Keyword.get(opts, :http_timeout_ms, @default_http_timeout_ms),
        offset: 0,
        safe_offset: 0,
        buffer: "",
        offset_path: offset_path(),
        last_snapshot_at: nil,
        last_poll_at: nil,
        last_error: nil
      }

      {:ok, load_offset(state), {:continue, :bootstrap}}
    else
      {:stop, :missing_leader_url}
    end
  end

  @impl true
  def handle_continue(:bootstrap, state) do
    {state, result} = bootstrap(state)

    case result do
      :ok ->
        schedule_poll(state)
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("Replica bootstrap failed: #{inspect(reason)}")
        state = %{state | last_error: reason}
        schedule_poll(state)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    state =
      case poll_aof(state) do
        {:ok, new_state} ->
          new_state

        {:error, reason} ->
          Logger.warning("Replica poll failed: #{inspect(reason)}")
          %{state | last_error: reason}
      end

    schedule_poll(state)
    {:noreply, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    reply = %{
      leader_url: state.leader_url,
      offset: state.offset,
      safe_offset: state.safe_offset,
      last_snapshot_at: state.last_snapshot_at,
      last_poll_at: state.last_poll_at,
      last_error: state.last_error,
      poll_interval_ms: state.poll_interval_ms,
      http_timeout_ms: state.http_timeout_ms
    }

    {:reply, {:ok, reply}, state}
  end

  defp ensure_store do
    case GenServer.whereis(KVStore) do
      nil -> KVStore.start_link()
      _pid -> :ok
    end
  end

  defp bootstrap(state) do
    case fetch_snapshot(state) do
      {:ok, payload} ->
        case KVStore.replace_snapshot(payload) do
          :ok ->
            state = %{
              state
              | offset: 0,
                safe_offset: 0,
                buffer: "",
                last_snapshot_at: System.system_time(:millisecond)
            }

            {persist_offset(state), :ok}

          {:error, reason} ->
            Logger.warning("Replica snapshot apply failed: #{inspect(reason)}")
            {state, {:error, reason}}
        end

      {:error, reason} ->
        {state, {:error, reason}}
    end
  end

  defp fetch_snapshot(state) do
    url = state.leader_url <> "/replication/snapshot"

    case http_get(url, state.http_timeout_ms, auth_headers()) do
      {:ok, 200, _headers, body} ->
        case Jason.decode(body) do
          {:ok, payload} -> {:ok, payload}
          {:error, reason} -> {:error, reason}
        end

      {:ok, status, _headers, body} ->
        {:error, {:snapshot_failed, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp poll_aof(state) do
    url =
      state.leader_url <>
        "/replication/aof?since=" <>
        Integer.to_string(state.offset) <>
        "&max_bytes=65536"

    case http_get(url, state.http_timeout_ms, auth_headers()) do
      {:ok, 204, headers, _body} ->
        response_size = parse_aof_size(headers, state.offset)
        next_offset = max(state.offset, response_size)

        state =
          state
          |> Map.put(:offset, next_offset)
          |> Map.put(:safe_offset, min(state.safe_offset, next_offset))
          |> Map.put(:last_poll_at, System.system_time(:millisecond))

        {:ok, persist_offset(state)}

      {:ok, 200, headers, body} ->
        {buffer, safe_offset} = apply_aof_body(body, state)
        next_offset = state.offset + byte_size(body)
        response_size = parse_aof_size(headers, next_offset)
        next_offset = min(next_offset, response_size)
        safe_offset = min(safe_offset, next_offset)

        state =
          state
          |> Map.put(:offset, next_offset)
          |> Map.put(:safe_offset, safe_offset)
          |> Map.put(:buffer, buffer)
          |> Map.put(:last_poll_at, System.system_time(:millisecond))

        state = persist_offset(state)

        {:ok, state}

      {:ok, status, _headers, body} ->
        {:error, {:aof_failed, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp apply_aof_body(body, state) do
    data = state.buffer <> body
    {lines, buffer} = split_aof_lines(data)

    Enum.each(lines, &apply_aof_line/1)

    safe_offset = state.offset + byte_size(body) - byte_size(buffer)
    {buffer, safe_offset}
  end

  defp split_aof_lines(data) do
    parts = String.split(data, "\n", trim: false)

    case List.last(parts) do
      "" -> {Enum.drop(parts, -1), ""}
      last -> {Enum.drop(parts, -1), last}
    end
  end

  defp apply_aof_line(""), do: :ok
  defp apply_aof_line(line) do
    case Jason.decode(line) do
      {:ok, record} -> KVStore.apply_replication(record)
      {:error, _} -> :ok
    end
  end

  defp parse_aof_size(headers, fallback) do
    case get_header(headers, "x-toska-aof-size") do
      nil ->
        fallback

      value ->
        case Integer.parse(value) do
          {int, ""} -> int
          _ -> fallback
        end
    end
  end

  defp get_header(headers, key) do
    key = String.downcase(key)

    headers
    |> Enum.find_value(fn {header, value} ->
      if String.downcase(to_string(header)) == key do
        to_string(value)
      end
    end)
  end

  defp http_get(url, timeout_ms, headers) do
    request = {to_charlist(url), headers}
    http_options = [timeout: timeout_ms, connect_timeout: timeout_ms]
    options = [body_format: :binary]

    case :httpc.request(:get, request, http_options, options) do
      {:ok, {{_http_version, status, _reason}, headers, body}} ->
        {:ok, status, headers, body}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp auth_headers do
    token = ConfigManager.cached_replication_auth_token()

    if is_binary(token) and token != "" do
      bearer = "Bearer " <> token
      [{~c"authorization", to_charlist(bearer)}]
    else
      []
    end
  end

  defp schedule_poll(state) do
    Process.send_after(self(), :poll, state.poll_interval_ms)
  end

  defp ensure_http do
    _ = Application.ensure_all_started(:inets)
    _ = Application.ensure_all_started(:ssl)
    :ok
  end

  defp offset_path do
    case System.get_env("TOSKA_DATA_DIR") do
      nil -> Path.join([data_dir(), "replica.offset"])
      "" -> Path.join([data_dir(), "replica.offset"])
      dir -> Path.join([dir, "replica.offset"])
    end
  end

  defp data_dir do
    case GenServer.whereis(ConfigManager) do
      nil ->
        default_data_dir()

      _pid ->
        case ConfigManager.list() do
          {:ok, config} -> config["data_dir"] || default_data_dir()
          _ -> default_data_dir()
        end
    end
  end

  defp default_data_dir do
    Path.join([ConfigManager.config_dir(), "data"])
  end

  defp load_offset(state) do
    case File.read(state.offset_path) do
      {:ok, content} ->
        case Integer.parse(String.trim(content)) do
          {value, ""} ->
            value = max(value, 0)
            %{state | offset: value, safe_offset: value}

          _ -> state
        end

      _ ->
        state
    end
  end

  defp persist_offset(state) do
    File.mkdir_p!(Path.dirname(state.offset_path))
    File.write(state.offset_path, Integer.to_string(state.safe_offset))
    state
  end
end
