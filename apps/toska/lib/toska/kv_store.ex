defmodule Toska.KVStore do
  @moduledoc """
  Durable key/value store backed by ETS with JSON AOF and snapshots.
  """

  use GenServer
  require Logger

  alias Toska.ConfigManager

  @table :toska_kv
  @default_sync_mode :interval
  @default_sync_interval_ms 1000
  @default_snapshot_interval_ms 60_000
  @default_ttl_check_interval_ms 1000
  @default_compaction_interval_ms 300_000
  @default_compaction_aof_bytes 10_485_760
  @default_aof_file "toska.aof"
  @default_snapshot_file "toska_snapshot.json"
  @snapshot_version 1
  @aof_version 1

  # Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(key) when is_binary(key) do
    case :ets.whereis(@table) do
      :undefined ->
        {:error, :not_running}

      _ ->
        lookup_key(key, now_ms())
    end
  end

  def get(_), do: {:error, :invalid_key}

  def put(key, value, ttl_ms \\ nil)

  def put(key, value, ttl_ms) when is_binary(key) and is_binary(value) do
    call_store({:put, key, value, ttl_ms})
  end

  def put(_, _, _), do: {:error, :invalid_payload}

  def delete(key) when is_binary(key) do
    call_store({:delete, key})
  end

  def delete(_), do: {:error, :invalid_key}

  def mget(keys) when is_list(keys) do
    case :ets.whereis(@table) do
      :undefined ->
        {:error, :not_running}

      _ ->
        now = now_ms()

        values =
          keys
          |> Enum.map(fn key ->
            case key do
              k when is_binary(k) ->
                case lookup_key(k, now) do
                  {:ok, value} -> {k, value}
                  _ -> {k, nil}
                end

              _ ->
                {key, nil}
            end
          end)
          |> Map.new()

        {:ok, values}
    end
  end

  def mget(_), do: {:error, :invalid_keys}

  def list_keys(prefix \\ "", limit \\ 100)

  def list_keys(prefix, limit) when is_binary(prefix) and is_integer(limit) and limit >= 0 do
    case list_keys_cursor(prefix, limit, nil) do
      {:ok, %{keys: keys}} -> {:ok, keys}
      other -> other
    end
  end

  def list_keys(_, _), do: {:error, :invalid_prefix}

  @doc """
  List keys with cursor-based pagination.

  Returns `{:ok, %{keys: [String.t()], next_cursor: String.t() | nil}}`.

  ## Options

  - `prefix` - Only return keys starting with this prefix (default: "")
  - `limit` - Maximum number of keys to return (default: 100)
  - `cursor` - Cursor from a previous call to continue iteration

  ## Examples

      iex> Toska.KVStore.list_keys_cursor("user:", 10, nil)
      {:ok, %{keys: ["user:1", "user:2"], next_cursor: "..."}}

      iex> Toska.KVStore.list_keys_cursor("user:", 10, cursor)
      {:ok, %{keys: ["user:11", "user:12"], next_cursor: nil}}

  """
  def list_keys_cursor(prefix \\ "", limit \\ 100, cursor \\ nil)

  def list_keys_cursor(prefix, limit, cursor)
      when is_binary(prefix) and is_integer(limit) and limit >= 0 do
    case :ets.whereis(@table) do
      :undefined ->
        {:error, :not_running}

      _ ->
        if limit == 0 do
          {:ok, %{keys: [], next_cursor: nil}}
        else
          case decode_cursor_key(cursor, prefix) do
            {:error, :invalid_cursor} ->
              {:error, :invalid_cursor}

            {:ok, start_key} ->
              now = now_ms()
              chunk_size = max(min(limit + 1, 1000), 100)

              {keys, has_more} = collect_keys_after(prefix, start_key, limit, now, chunk_size)

              next_cursor =
                if has_more and length(keys) == limit do
                  last_key = List.last(keys)
                  Toska.Cursor.encode(last_key, prefix)
                else
                  nil
                end

              {:ok, %{keys: keys, next_cursor: next_cursor}}
          end
        end
    end
  end

  def list_keys_cursor(_, _, _), do: {:error, :invalid_args}

  def stats do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :stats)
    end
  end

  def snapshot do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :snapshot)
    end
  end

  def stop do
    case GenServer.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.call(__MODULE__, :stop)
    end
  end

  def replication_info do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :replication_info)
    end
  end

  def snapshot_path do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :snapshot_path)
    end
  end

  def aof_path do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :aof_path)
    end
  end

  def replace_snapshot(payload) when is_map(payload) do
    call_store({:replace_snapshot, payload})
  end

  def replace_snapshot(_), do: {:error, :invalid_snapshot}

  def apply_replication(records) when is_list(records) do
    call_store({:apply_replication, records})
  end

  def apply_replication(record) when is_map(record) do
    apply_replication([record])
  end

  def apply_replication(_), do: {:error, :invalid_replication_record}

  def compact do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, :compact)
    end
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    ensure_table()
    config = load_config()

    File.mkdir_p!(config.data_dir)

    snapshot_meta = load_snapshot(config.snapshot_path)
    replay_aof(config.aof_path)

    {:ok, aof_io} = File.open(config.aof_path, [:append, :utf8])

    state =
      config
      |> Map.put(:aof_io, aof_io)
      |> Map.put(:last_snapshot_at, snapshot_meta && snapshot_meta.created_at)
      |> Map.put(:last_snapshot_checksum, snapshot_meta && snapshot_meta.checksum)
      |> Map.put(:last_sync_at, nil)

    schedule_sync(state)
    schedule_snapshot(state)
    schedule_ttl_cleanup(state)
    schedule_compaction(state)

    Logger.info("KV store ready (AOF: #{config.aof_path})")

    {:ok, state}
  end

  @impl true
  def handle_call({:put, key, value, ttl_ms}, _from, state) do
    now = now_ms()
    expires_at = normalize_ttl(ttl_ms, now)

    if expires_at == :expired do
      :ets.delete(@table, key)
      append_aof(state, %{op: "del", key: key})
      {:reply, :ok, maybe_sync(state)}
    else
      :ets.insert(@table, {key, value, expires_at})
      append_aof(state, %{op: "set", key: key, value: value, expires_at: expires_at})
      {:reply, :ok, maybe_sync(state)}
    end
  end

  def handle_call({:delete, key}, _from, state) do
    :ets.delete(@table, key)
    append_aof(state, %{op: "del", key: key})
    {:reply, :ok, maybe_sync(state)}
  end

  def handle_call(:stats, _from, state) do
    table_info =
      case :ets.whereis(@table) do
        :undefined -> %{size: 0, memory: 0}
        _ -> %{size: :ets.info(@table, :size), memory: :ets.info(@table, :memory)}
      end

    reply = %{
      keys: table_info.size,
      memory_words: table_info.memory,
      aof_path: state.aof_path,
      aof_bytes: file_size(state.aof_path),
      snapshot_path: state.snapshot_path,
      snapshot_bytes: file_size(state.snapshot_path),
      snapshot_checksum: state.last_snapshot_checksum,
      snapshot_version: @snapshot_version,
      aof_version: @aof_version,
      sync_mode: Atom.to_string(state.sync_mode),
      sync_interval_ms: state.sync_interval_ms,
      snapshot_interval_ms: state.snapshot_interval_ms,
      ttl_check_interval_ms: state.ttl_check_interval_ms,
      compaction_interval_ms: state.compaction_interval_ms,
      compaction_aof_bytes: state.compaction_aof_bytes,
      last_snapshot_at: state.last_snapshot_at,
      last_sync_at: state.last_sync_at
    }

    {:reply, {:ok, reply}, state}
  end

  def handle_call(:snapshot, _from, state) do
    case write_snapshot(state.snapshot_path) do
      {:ok, checksum} ->
        state = reset_aof(state)
        {:reply, :ok, %{state | last_snapshot_at: now_ms(), last_snapshot_checksum: checksum}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:compact, _from, state) do
    {:reply, :ok, maybe_compact(state, true)}
  end

  def handle_call(:stop, _from, state) do
    state = flush_and_close(state)
    {:stop, :normal, :ok, state}
  end

  def handle_call(:replication_info, _from, state) do
    info = %{
      snapshot_path: state.snapshot_path,
      snapshot_checksum: state.last_snapshot_checksum,
      snapshot_created_at: state.last_snapshot_at,
      snapshot_version: @snapshot_version,
      aof_path: state.aof_path,
      aof_size: file_size(state.aof_path),
      aof_version: @aof_version
    }

    {:reply, {:ok, info}, state}
  end

  def handle_call(:snapshot_path, _from, state) do
    {:reply, {:ok, state.snapshot_path}, state}
  end

  def handle_call(:aof_path, _from, state) do
    {:reply, {:ok, state.aof_path}, state}
  end

  def handle_call({:replace_snapshot, payload}, _from, state) do
    data =
      case payload do
        %{"data" => data} when is_map(data) -> data
        data when is_map(data) -> data
        _ -> nil
      end

    cond do
      is_nil(data) ->
        {:reply, {:error, :invalid_snapshot}, state}

      not valid_snapshot_checksum?(payload) ->
        {:reply, {:error, :invalid_checksum}, state}

      true ->
        :ets.delete_all_objects(@table)
        load_entries(data, now_ms())

        case write_snapshot(state.snapshot_path) do
          {:ok, checksum} ->
            state = reset_aof(state)
            updated = %{state | last_snapshot_at: now_ms(), last_snapshot_checksum: checksum}
            {:reply, :ok, updated}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:apply_replication, records}, _from, state) do
    now = now_ms()

    Enum.each(records, fn record ->
      if valid_aof_checksum?(record) do
        apply_aof_record(record, now)
        append_aof(state, record)
      end
    end)

    {:reply, :ok, maybe_sync(state)}
  end

  @impl true
  def handle_info(:sync_aof, state) do
    state = sync_aof(state)
    schedule_sync(state)
    {:noreply, state}
  end

  def handle_info(:snapshot, state) do
    state =
      case write_snapshot(state.snapshot_path) do
        {:ok, checksum} ->
          reset_aof(%{
            state
            | last_snapshot_at: now_ms(),
              last_snapshot_checksum: checksum
          })

        {:error, reason} ->
          Logger.warning("Snapshot failed: #{inspect(reason)}")
          state
      end

    schedule_snapshot(state)
    {:noreply, state}
  end

  def handle_info(:ttl_cleanup, state) do
    cleanup_expired(now_ms())
    schedule_ttl_cleanup(state)
    {:noreply, state}
  end

  def handle_info(:compact, state) do
    state = maybe_compact(state, false)
    schedule_compaction(state)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    flush_and_close(state)
    :ok
  end

  # Internal helpers

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined -> :ok
      tid -> :ets.delete(tid)
    end

    :ets.new(@table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp lookup_key(key, now) do
    case :ets.lookup(@table, key) do
      [{^key, value, expires_at}] ->
        if expired?(expires_at, now) do
          :ets.delete(@table, key)
          {:error, :not_found}
        else
          {:ok, value}
        end

      _ ->
        {:error, :not_found}
    end
  end

  defp expired?(nil, _now), do: false
  defp expired?(expires_at, now) when is_integer(expires_at), do: expires_at <= now
  defp expired?(_, _now), do: false

  defp normalize_ttl(nil, _now), do: nil

  defp normalize_ttl(ttl_ms, now) when is_integer(ttl_ms) do
    if ttl_ms <= 0 do
      :expired
    else
      now + ttl_ms
    end
  end

  defp normalize_ttl(ttl_ms, now) when is_binary(ttl_ms) do
    case Integer.parse(ttl_ms) do
      {value, ""} -> normalize_ttl(value, now)
      _ -> nil
    end
  end

  defp normalize_ttl(_ttl_ms, _now), do: nil

  defp cleanup_expired(now) do
    match_spec = [
      {
        {:"$1", :"$2", :"$3"},
        [{:is_integer, :"$3"}, {:"=<", :"$3", now}],
        [:"$1"]
      }
    ]

    :ets.select(@table, match_spec)
    |> Enum.each(&:ets.delete(@table, &1))
  end

  defp load_snapshot(path) do
    case File.read(path) do
      {:ok, content} ->
        now = now_ms()

        case Jason.decode(content) do
          {:ok, %{"data" => data} = payload} when is_map(data) ->
            if valid_snapshot_checksum?(payload) do
              load_entries(data, now)

              %{
                checksum: Map.get(payload, "checksum"),
                created_at: Map.get(payload, "created_at")
              }
            else
              Logger.warning("Snapshot checksum mismatch, skipping load")
              nil
            end

          {:ok, data} when is_map(data) ->
            load_entries(data, now)
            nil

          {:error, reason} ->
            Logger.warning("Failed to decode snapshot: #{inspect(reason)}")
            nil
        end

      {:error, :enoent} ->
        nil

      {:error, reason} ->
        Logger.warning("Failed to read snapshot: #{inspect(reason)}")
        nil
    end
  end

  defp load_entries(data, now) do
    Enum.each(data, fn {key, entry} ->
      case entry do
        %{"value" => value} = map ->
          expires_at = Map.get(map, "expires_at")

          unless expired?(expires_at, now) do
            :ets.insert(@table, {key, value, expires_at})
          end

        value when is_binary(value) ->
          :ets.insert(@table, {key, value, nil})

        _ ->
          :ok
      end
    end)
  end

  defp replay_aof(path) do
    case File.open(path, [:read]) do
      {:ok, io} ->
        now = now_ms()

        io
        |> IO.stream(:line)
        |> Stream.map(&String.trim/1)
        |> Stream.reject(&(&1 == ""))
        |> Enum.each(fn line ->
          case Jason.decode(line) do
            {:ok, record} ->
              if valid_aof_checksum?(record) do
                apply_aof_record(record, now)
              else
                Logger.warning("Skipping AOF entry with invalid checksum")
              end

            {:error, reason} ->
              Logger.warning("Skipping invalid AOF line: #{inspect(reason)}")
          end
        end)

        File.close(io)

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to read AOF: #{inspect(reason)}")
    end
  end

  # Cursor-based key collection helpers

  defp decode_cursor_key(nil, _prefix), do: {:ok, nil}
  defp decode_cursor_key("", _prefix), do: {:ok, nil}

  defp decode_cursor_key(cursor, expected_prefix) do
    case Toska.Cursor.decode(cursor) do
      {:ok, {key, ^expected_prefix}} -> {:ok, key}
      {:ok, {_key, _different_prefix}} -> {:error, :invalid_cursor}
      {:error, _} -> {:error, :invalid_cursor}
    end
  end

  defp collect_keys_after(prefix, start_key, limit, now, _chunk_size) do
    # Collect all matching keys, sort them, then paginate
    # This is necessary because ETS doesn't iterate in sorted order,
    # so cursor-based pagination requires sorting first
    all_keys = collect_all_matching_keys(prefix, now)

    # Sort keys for consistent pagination
    sorted_keys = Enum.sort(all_keys)

    # Filter to keys after cursor
    filtered_keys =
      case start_key do
        nil -> sorted_keys
        key -> Enum.filter(sorted_keys, &(&1 > key))
      end

    # Take limit+1 to detect if there are more
    taken = Enum.take(filtered_keys, limit + 1)

    if length(taken) > limit do
      {Enum.take(taken, limit), true}
    else
      {taken, false}
    end
  end

  defp collect_all_matching_keys(prefix, now) do
    match_spec = [{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}]

    case :ets.select(@table, match_spec, 1000) do
      :"$end_of_table" ->
        []

      {rows, continuation} ->
        collect_all_matching_from(rows, continuation, prefix, now, [])
    end
  end

  defp collect_all_matching_from(rows, continuation, prefix, now, acc) do
    acc =
      Enum.reduce(rows, acc, fn {key, expires_at}, acc ->
        cond do
          expired?(expires_at, now) ->
            :ets.delete(@table, key)
            acc

          not matches_prefix?(key, prefix) ->
            acc

          true ->
            [key | acc]
        end
      end)

    case continuation do
      :"$end_of_table" ->
        acc

      _ ->
        case :ets.select(continuation) do
          :"$end_of_table" ->
            acc

          {next_rows, next_continuation} ->
            collect_all_matching_from(next_rows, next_continuation, prefix, now, acc)
        end
    end
  end

  defp matches_prefix?(_key, ""), do: true
  defp matches_prefix?(key, prefix), do: String.starts_with?(key, prefix)

  defp append_aof(state, record) do
    if state.aof_io do
      record = normalize_aof_record(record)
      json = Jason.encode!(record)

      case IO.binwrite(state.aof_io, json <> "\n") do
        :ok -> :ok
        {:error, reason} -> Logger.warning("AOF write failed: #{inspect(reason)}")
      end
    end
  end

  defp write_snapshot(path) do
    now = now_ms()

    data =
      :ets.tab2list(@table)
      |> Enum.reduce(%{}, fn {key, value, expires_at}, acc ->
        if expired?(expires_at, now) do
          acc
        else
          Map.put(acc, key, %{"value" => value, "expires_at" => expires_at})
        end
      end)

    checksum = snapshot_checksum(data)

    payload = %{
      "version" => @snapshot_version,
      "created_at" => now,
      "checksum" => checksum,
      "data" => data
    }

    tmp_path = path <> ".tmp"

    with {:ok, json} <- Jason.encode(payload, pretty: true),
         :ok <- File.write(tmp_path, json),
         :ok <- File.rename(tmp_path, path) do
      {:ok, checksum}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp reset_aof(state) do
    flush_and_close(state)
    {:ok, truncate_io} = File.open(state.aof_path, [:write, :utf8])
    File.close(truncate_io)
    {:ok, aof_io} = File.open(state.aof_path, [:append, :utf8])
    %{state | aof_io: aof_io}
  end

  defp flush_and_close(state) do
    state = sync_aof(state)
    if state.aof_io, do: File.close(state.aof_io)
    %{state | aof_io: nil}
  end

  defp schedule_sync(state) do
    if state.sync_mode == :interval do
      Process.send_after(self(), :sync_aof, state.sync_interval_ms)
    end
  end

  defp schedule_snapshot(state) do
    Process.send_after(self(), :snapshot, state.snapshot_interval_ms)
  end

  defp schedule_ttl_cleanup(state) do
    Process.send_after(self(), :ttl_cleanup, state.ttl_check_interval_ms)
  end

  defp schedule_compaction(state) do
    Process.send_after(self(), :compact, state.compaction_interval_ms)
  end

  defp maybe_compact(state, force) do
    aof_bytes = file_size(state.aof_path)

    if force or aof_bytes >= state.compaction_aof_bytes do
      case write_snapshot(state.snapshot_path) do
        {:ok, checksum} ->
          reset_aof(%{
            state
            | last_snapshot_at: now_ms(),
              last_snapshot_checksum: checksum
          })

        {:error, reason} ->
          Logger.warning("Compaction snapshot failed: #{inspect(reason)}")
          state
      end
    else
      state
    end
  end

  defp sync_aof(state) do
    if state.aof_io do
      case :file.sync(state.aof_io) do
        :ok ->
          %{state | last_sync_at: now_ms()}

        {:error, reason} ->
          Logger.warning("AOF sync failed: #{inspect(reason)}")
          state
      end
    else
      state
    end
  end

  defp maybe_sync(state) do
    if state.sync_mode == :always do
      sync_aof(state)
    else
      state
    end
  end

  defp load_config do
    defaults = %{
      data_dir: default_data_dir(),
      aof_file: @default_aof_file,
      snapshot_file: @default_snapshot_file,
      sync_mode: @default_sync_mode,
      sync_interval_ms: @default_sync_interval_ms,
      snapshot_interval_ms: @default_snapshot_interval_ms,
      ttl_check_interval_ms: @default_ttl_check_interval_ms,
      compaction_interval_ms: @default_compaction_interval_ms,
      compaction_aof_bytes: @default_compaction_aof_bytes
    }

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

    data_dir =
      System.get_env("TOSKA_DATA_DIR") ||
        config["data_dir"] ||
        defaults.data_dir

    %{
      data_dir: data_dir,
      aof_path: Path.join(data_dir, config["aof_file"] || defaults.aof_file),
      snapshot_path: Path.join(data_dir, config["snapshot_file"] || defaults.snapshot_file),
      sync_mode: parse_sync_mode(config["sync_mode"], defaults.sync_mode),
      sync_interval_ms: parse_int(config["sync_interval_ms"], defaults.sync_interval_ms),
      snapshot_interval_ms:
        parse_int(config["snapshot_interval_ms"], defaults.snapshot_interval_ms),
      ttl_check_interval_ms:
        parse_int(config["ttl_check_interval_ms"], defaults.ttl_check_interval_ms),
      compaction_interval_ms:
        parse_int(config["compaction_interval_ms"], defaults.compaction_interval_ms),
      compaction_aof_bytes:
        parse_int(config["compaction_aof_bytes"], defaults.compaction_aof_bytes)
    }
  end

  defp parse_sync_mode(nil, default), do: default

  defp parse_sync_mode(mode, default) when is_binary(mode) do
    case String.downcase(mode) do
      "always" -> :always
      "interval" -> :interval
      "none" -> :none
      _ -> default
    end
  end

  defp parse_sync_mode(mode, _default) when is_atom(mode), do: mode
  defp parse_sync_mode(_, default), do: default

  defp parse_int(nil, default), do: default
  defp parse_int(value, _default) when is_integer(value) and value > 0, do: value

  defp parse_int(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> default
    end
  end

  defp parse_int(_, default), do: default

  defp default_data_dir do
    base = ConfigManager.config_dir()
    Path.join([base, "data"])
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %File.Stat{size: size}} -> size
      _ -> 0
    end
  end

  defp now_ms do
    System.system_time(:millisecond)
  end

  defp apply_aof_record(%{"op" => "set", "key" => key, "value" => value} = record, now) do
    expires_at = Map.get(record, "expires_at")

    unless expired?(expires_at, now) do
      :ets.insert(@table, {key, value, expires_at})
    end
  end

  defp apply_aof_record(%{"op" => "del", "key" => key}, _now) do
    :ets.delete(@table, key)
  end

  defp apply_aof_record(_record, _now), do: :ok

  defp normalize_aof_record(record) do
    base =
      record
      |> Map.new(fn {key, value} -> {to_string(key), value} end)
      |> Map.drop(["checksum"])
      |> Map.put("v", @aof_version)

    checksum = aof_checksum(base)
    Map.put(base, "checksum", checksum)
  end

  defp valid_aof_checksum?(record) do
    checksum = Map.get(record, "checksum")

    if is_binary(checksum) do
      base = Map.drop(record, ["checksum"])
      checksum == aof_checksum(base)
    else
      true
    end
  end

  defp valid_snapshot_checksum?(%{"checksum" => checksum, "data" => data})
       when is_binary(checksum) do
    checksum == snapshot_checksum(data)
  end

  defp valid_snapshot_checksum?(_), do: true

  defp snapshot_checksum(data) when is_map(data) do
    data
    |> canonical_json()
    |> sha256_hex()
  end

  defp aof_checksum(record) when is_map(record) do
    record
    |> canonical_json()
    |> sha256_hex()
  end

  defp sha256_hex(data) when is_binary(data) do
    :crypto.hash(:sha256, data)
    |> Base.encode16(case: :lower)
  end

  defp canonical_json(term) do
    term
    |> canonical_term()
    |> Jason.encode!()
  end

  defp canonical_term(term) when is_map(term) do
    term
    |> Enum.map(fn {key, value} -> [to_string(key), canonical_term(value)] end)
    |> Enum.sort_by(&List.first/1)
  end

  defp canonical_term(term) when is_list(term) do
    Enum.map(term, &canonical_term/1)
  end

  defp canonical_term(term), do: term

  defp call_store(message) do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :not_running}
      _pid -> GenServer.call(__MODULE__, message)
    end
  end
end
