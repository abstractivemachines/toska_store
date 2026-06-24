defmodule Toska.KVStore do
  @moduledoc """
  Durable key/value store backed by ETS with JSON AOF and snapshots.
  """

  use GenServer
  require Logger

  alias Toska.ConfigManager

  @table :toska_kv
  @key_index_table :toska_kv_keys
  @default_sync_mode :interval
  @default_sync_interval_ms 1000
  @default_snapshot_interval_ms 60_000
  @default_ttl_check_interval_ms 1000
  @default_compaction_interval_ms 300_000
  @default_compaction_aof_bytes 10_485_760
  @default_watch_history_limit 10_000
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
        case lookup_entry(key, now_ms()) do
          {:ok, entry} -> {:ok, entry.value}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  def get(_), do: {:error, :invalid_key}

  def get_entry(key) when is_binary(key) do
    case :ets.whereis(@table) do
      :undefined ->
        {:error, :not_running}

      _ ->
        lookup_entry(key, now_ms())
    end
  end

  def get_entry(_), do: {:error, :invalid_key}

  def put(key, value, ttl_ms \\ nil)

  def put(key, value, ttl_ms) when is_binary(key) and is_binary(value) do
    put(key, value, ttl_ms, [])
  end

  def put(_, _, _), do: {:error, :invalid_payload}

  def put(key, value, ttl_ms, opts) when is_binary(key) and is_binary(value) do
    with {:ok, write_opts} <- normalize_put_options(opts) do
      call_store({:put, key, value, ttl_ms, write_opts.conditions, write_opts.lease_id})
    end
  end

  def put(_, _, _, _), do: {:error, :invalid_payload}

  def delete(key, opts \\ [])

  def delete(key, opts) when is_binary(key) do
    with {:ok, conditions} <- normalize_conditions(opts) do
      call_store({:delete, key, conditions})
    end
  end

  def delete(_, _), do: {:error, :invalid_key}

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
                case lookup_entry(k, now) do
                  {:ok, entry} -> {k, entry.value}
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
    case list_range(prefix, limit, cursor: cursor) do
      {:ok, %{items: items, next_cursor: next_cursor}} ->
        {:ok, %{keys: Enum.map(items, & &1.key), next_cursor: next_cursor}}

      other ->
        other
    end
  end

  def list_keys_cursor(_, _, _), do: {:error, :invalid_args}

  @doc """
  List a lexicographic key range with cursor-based pagination.

  Returns `{:ok, %{items: [map()], next_cursor: String.t() | nil}}`.

  ## Options

  - `cursor` - Cursor from a previous call to continue iteration
  - `start` - Inclusive lower-bound key for the first page
  - `include_values` - Include values in returned items
  - `include_metadata` - Include entry metadata in returned items
  """
  def list_range(prefix \\ "", limit \\ 100, opts \\ [])

  def list_range(prefix, limit, opts)
      when is_binary(prefix) and is_integer(limit) and limit >= 0 do
    with :ok <- ensure_read_tables(),
         {:ok, opts} <- normalize_range_options(opts),
         {:ok, cursor_key} <- decode_cursor_key(opts.cursor, prefix) do
      if limit == 0 do
        {:ok, %{items: [], next_cursor: nil}}
      else
        now = now_ms()
        start = range_start(prefix, opts.start, cursor_key)
        {entries, has_more} = collect_range_entries(prefix, start, limit, now)

        next_cursor =
          if has_more and length(entries) == limit do
            entries
            |> List.last()
            |> Map.fetch!(:key)
            |> Toska.Cursor.encode(prefix)
          else
            nil
          end

        items = Enum.map(entries, &range_item(&1, opts))
        {:ok, %{items: items, next_cursor: next_cursor}}
      end
    end
  end

  def list_range(_, _, _), do: {:error, :invalid_args}

  def txn(compare, success, failure \\ [])

  def txn(compare, success, failure)
      when is_list(compare) and is_list(success) and is_list(failure) do
    with {:ok, txn} <- normalize_txn(compare, success, failure) do
      call_store({:txn, txn})
    end
  end

  def txn(_, _, _), do: {:error, :invalid_transaction}

  def watch(prefix \\ "", since_revision \\ nil, opts \\ [])

  def watch(prefix, since_revision, opts) when is_binary(prefix) and is_list(opts) do
    with {:ok, since_revision} <- normalize_since_revision(since_revision) do
      pid = Keyword.get(opts, :pid, self())
      call_store({:watch, prefix, since_revision, pid})
    end
  end

  def watch(_, _, _), do: {:error, :invalid_watch}

  def unwatch(ref) when is_reference(ref) do
    call_store({:unwatch, ref})
  end

  def unwatch(_), do: {:error, :invalid_watch}

  def create_lease(ttl_ms, opts \\ []) do
    with {:ok, ttl_ms} <- normalize_lease_ttl(ttl_ms),
         {:ok, lease_id} <- normalize_lease_create_id(opts) do
      call_store({:create_lease, ttl_ms, lease_id})
    else
      _ -> {:error, :invalid_lease}
    end
  end

  def keepalive_lease(id) do
    with {:ok, id} <- normalize_required_id(id) do
      call_store({:keepalive_lease, id})
    else
      _ -> {:error, :invalid_lease}
    end
  end

  def delete_lease(id) do
    with {:ok, id} <- normalize_required_id(id) do
      call_store({:delete_lease, id})
    else
      _ -> {:error, :invalid_lease}
    end
  end

  def acquire_lock(name, lease_id, holder \\ nil) do
    with {:ok, name} <- normalize_required_id(name),
         {:ok, lease_id} <- normalize_required_id(lease_id),
         {:ok, holder} <- normalize_lock_holder(holder) do
      call_store({:acquire_lock, name, lease_id, holder})
    else
      _ -> {:error, :invalid_lock}
    end
  end

  def release_lock(name, lease_id) do
    with {:ok, name} <- normalize_required_id(name),
         {:ok, lease_id} <- normalize_required_id(lease_id) do
      call_store({:release_lock, name, lease_id})
    else
      _ -> {:error, :invalid_lock}
    end
  end

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
    ensure_tables()
    config = load_config()

    File.mkdir_p!(config.data_dir)

    snapshot_meta = load_snapshot(config.snapshot_path)
    snapshot_revision = (snapshot_meta && snapshot_meta.store_revision) || 0

    aof_meta =
      replay_aof(
        config.aof_path,
        snapshot_revision,
        config.watch_history_limit,
        (snapshot_meta && snapshot_meta.leases) || %{},
        (snapshot_meta && snapshot_meta.locks) || %{}
      )

    {:ok, aof_io} = File.open(config.aof_path, [:append, :utf8])

    state =
      config
      |> Map.put(:aof_io, aof_io)
      |> Map.put(:last_snapshot_at, snapshot_meta && snapshot_meta.created_at)
      |> Map.put(:last_snapshot_checksum, snapshot_meta && snapshot_meta.checksum)
      |> Map.put(:last_sync_at, nil)
      |> Map.put(:revision, aof_meta.revision)
      |> Map.put(:watch_events, aof_meta.events)
      |> Map.put(:watchers, %{})
      |> Map.put(:leases, aof_meta.leases)
      |> Map.put(:locks, aof_meta.locks)

    schedule_sync(state)
    schedule_snapshot(state)
    schedule_ttl_cleanup(state)
    schedule_compaction(state)

    Logger.info("KV store ready (AOF: #{config.aof_path})")

    {:ok, state}
  end

  @impl true
  def handle_call({:put, key, value, ttl_ms, conditions, lease_id}, _from, state) do
    now = now_ms()
    current = current_entry(key, now)

    with true <- conditions_met?(current, conditions),
         {:ok, expires_at} <- write_expires_at(ttl_ms, lease_id, state, now) do
      state =
        if expires_at == :expired do
          delete_entry(state, key, current, now, "delete")
        else
          entry = next_entry(key, value, expires_at, current, now, lease_id)
          put_entry(state, entry, now)
        end

      {:reply, :ok, maybe_sync(state)}
    else
      false -> {:reply, {:error, :condition_failed}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, key, conditions}, _from, state) do
    now = now_ms()
    current = current_entry(key, now)

    if conditions_met?(current, conditions) do
      state = delete_entry(state, key, current, now, "delete")
      {:reply, :ok, maybe_sync(state)}
    else
      {:reply, {:error, :condition_failed}, state}
    end
  end

  def handle_call({:create_lease, ttl_ms, requested_id}, _from, state) do
    now = now_ms()
    state = drop_expired_lease_id(state, requested_id, now)
    lease_id = requested_id || unique_lease_id(state.leases)

    if active_lease(state.leases, lease_id, now) do
      {:reply, {:error, :lease_exists}, state}
    else
      lease = lease(lease_id, ttl_ms, now, now + ttl_ms, now)

      state =
        state
        |> put_lease_state(lease)
        |> append_metadata_aof(lease_record("lease_create", lease))

      {:reply, {:ok, lease_result(lease)}, maybe_sync(state)}
    end
  end

  def handle_call({:keepalive_lease, lease_id}, _from, state) do
    now = now_ms()

    case active_lease(state.leases, lease_id, now) do
      nil ->
        {:reply, {:error, :lease_not_found}, state}

      current ->
        lease = %{current | expires_at: now + current.ttl_ms, updated_at: now}

        state =
          state
          |> put_lease_state(lease)
          |> renew_attached_locks(lease)
          |> append_metadata_aof(lease_record("lease_keepalive", lease))
          |> renew_attached_entries(lease, now)

        {:reply, {:ok, lease_result(lease)}, maybe_sync(state)}
    end
  end

  def handle_call({:delete_lease, lease_id}, _from, state) do
    now = now_ms()

    case active_lease(state.leases, lease_id, now) do
      nil ->
        {:reply, {:error, :lease_not_found}, state}

      lease ->
        state = revoke_lease(state, lease.id, now, "lease_delete", "delete")
        {:reply, :ok, maybe_sync(state)}
    end
  end

  def handle_call({:acquire_lock, name, lease_id, holder}, _from, state) do
    now = now_ms()

    case active_lease(state.leases, lease_id, now) do
      nil ->
        {:reply, {:error, :lease_not_found}, state}

      lease ->
        case active_lock(state, name, now) do
          nil ->
            lock = lock(name, lease.id, holder, now, lease.expires_at)

            state =
              state
              |> put_lock_state(lock)
              |> append_metadata_aof(lock_record("lock_acquire", lock))

            {:reply, {:ok, lock_result(lock)}, maybe_sync(state)}

          %{lease_id: ^lease_id} = lock ->
            {:reply, {:ok, lock_result(lock)}, state}

          _lock ->
            {:reply, {:error, :lock_held}, state}
        end
    end
  end

  def handle_call({:release_lock, name, lease_id}, _from, state) do
    now = now_ms()

    case active_lock(state, name, now) do
      nil ->
        state = remove_lock_state(state, name)
        {:reply, {:error, :lock_not_found}, state}

      %{lease_id: ^lease_id} ->
        state =
          state
          |> remove_lock_state(name)
          |> append_metadata_aof(%{op: "lock_release", name: name})

        {:reply, :ok, maybe_sync(state)}

      _lock ->
        {:reply, {:error, :lock_owner_mismatch}, state}
    end
  end

  def handle_call({:txn, txn}, _from, state) do
    now = now_ms()
    succeeded = Enum.all?(txn.compare, &compare_met?(&1, now))
    ops = if succeeded, do: txn.success, else: txn.failure
    initial_revision = state.revision
    {responses, state} = apply_txn_ops(ops, state, now)
    state = if state.revision != initial_revision, do: maybe_sync(state), else: state

    {:reply, {:ok, %{succeeded: succeeded, responses: responses}}, state}
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
      revision: state.revision,
      leases: active_lease_count(state.leases),
      locks: active_lock_count(state.locks, state.leases),
      watch_history_size: length(state.watch_events),
      watch_history_limit: state.watch_history_limit,
      watchers: map_size(state.watchers),
      last_snapshot_at: state.last_snapshot_at,
      last_sync_at: state.last_sync_at
    }

    {:reply, {:ok, reply}, state}
  end

  def handle_call(:snapshot, _from, state) do
    case write_snapshot(state) do
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
      aof_version: @aof_version,
      store_revision: state.revision
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
        delete_all_entries()
        now = now_ms()
        load_entries(data, now)
        leases = load_leases(Map.get(payload, "leases"), now)
        locks = load_locks(Map.get(payload, "locks"), leases, now)
        store_revision = snapshot_store_revision(payload, state.revision)

        updated = %{
          state
          | revision: store_revision,
            watch_events: [],
            leases: leases,
            locks: locks
        }

        case write_snapshot(updated) do
          {:ok, checksum} ->
            updated = reset_aof(updated)
            updated = %{updated | last_snapshot_at: now_ms(), last_snapshot_checksum: checksum}
            {:reply, :ok, updated}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:apply_replication, records}, _from, state) do
    now = now_ms()

    state =
      Enum.reduce(records, state, fn record, state ->
        if valid_aof_checksum?(record) do
          apply_replicated_record(record, state, now)
        else
          state
        end
      end)

    {:reply, :ok, maybe_sync(state)}
  end

  def handle_call({:watch, prefix, since_revision, pid}, _from, state) do
    case watch_replay(state, prefix, since_revision) do
      {:ok, events} ->
        ref = make_ref()
        monitor = Process.monitor(pid)

        watchers =
          Map.put(state.watchers, ref, %{
            pid: pid,
            prefix: prefix,
            monitor: monitor
          })

        reply = %{
          ref: ref,
          current_revision: state.revision,
          events: events
        }

        {:reply, {:ok, reply}, %{state | watchers: watchers}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:unwatch, ref}, _from, state) do
    {:reply, :ok, remove_watcher(state, ref)}
  end

  @impl true
  def handle_info(:sync_aof, state) do
    state = sync_aof(state)
    schedule_sync(state)
    {:noreply, state}
  end

  def handle_info(:snapshot, state) do
    state =
      case write_snapshot(state) do
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
    state = cleanup_expired(now_ms(), state)
    schedule_ttl_cleanup(state)
    {:noreply, state}
  end

  def handle_info(:compact, state) do
    state = maybe_compact(state, false)
    schedule_compaction(state)
    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    watchers =
      state.watchers
      |> Enum.reject(fn {_ref, watcher} -> watcher.monitor == monitor end)
      |> Map.new()

    {:noreply, %{state | watchers: watchers}}
  end

  @impl true
  def terminate(_reason, state) do
    flush_and_close(state)
    :ok
  end

  # Internal helpers

  defp ensure_tables do
    delete_table(@table)
    delete_table(@key_index_table)

    :ets.new(@table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@key_index_table, [
      :named_table,
      :ordered_set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp delete_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      tid -> :ets.delete(tid)
    end
  end

  defp ensure_read_tables do
    case {:ets.whereis(@table), :ets.whereis(@key_index_table)} do
      {:undefined, _} -> {:error, :not_running}
      {_, :undefined} -> {:error, :not_running}
      _ -> :ok
    end
  end

  defp lookup_entry(key, now) do
    case :ets.lookup(@table, key) do
      [tuple] ->
        entry = entry_from_tuple(tuple, now)

        if is_nil(entry) or expired?(entry.expires_at, now) do
          {:error, :not_found}
        else
          {:ok, entry}
        end

      _ ->
        {:error, :not_found}
    end
  end

  defp current_entry(key, now) do
    case lookup_entry(key, now) do
      {:ok, entry} -> entry
      {:error, :not_found} -> nil
    end
  end

  defp entry(key, value, expires_at, version, created_at, updated_at, lease_id \\ nil) do
    %{
      key: key,
      value: value,
      expires_at: expires_at,
      version: version,
      created_at: created_at,
      updated_at: updated_at,
      lease_id: lease_id
    }
  end

  defp entry_from_tuple({key, value, expires_at, version, created_at, updated_at, lease_id}, _now) do
    entry(key, value, expires_at, version, created_at, updated_at, lease_id)
  end

  defp entry_from_tuple({key, value, expires_at, version, created_at, updated_at}, _now) do
    entry(key, value, expires_at, version, created_at, updated_at)
  end

  defp entry_from_tuple({key, value, expires_at}, now) do
    entry(key, value, expires_at, 1, now, now)
  end

  defp entry_from_tuple(_tuple, _now), do: nil

  defp next_entry(key, value, expires_at, current, now, lease_id \\ nil)

  defp next_entry(key, value, expires_at, nil, now, lease_id) do
    entry(key, value, expires_at, 1, now, now, lease_id)
  end

  defp next_entry(key, value, expires_at, current, now, lease_id) do
    entry(key, value, expires_at, current.version + 1, current.created_at, now, lease_id)
  end

  defp insert_entry(entry) do
    :ets.insert(
      @table,
      {entry.key, entry.value, entry.expires_at, entry.version, entry.created_at,
       entry.updated_at, entry.lease_id}
    )

    :ets.insert(@key_index_table, {entry.key})
  end

  defp delete_stored_entry(key) do
    :ets.delete(@table, key)
    :ets.delete(@key_index_table, key)
  end

  defp delete_all_entries do
    :ets.delete_all_objects(@table)
    :ets.delete_all_objects(@key_index_table)
  end

  defp set_record(entry, revision) do
    %{
      op: "set",
      key: entry.key,
      value: entry.value,
      expires_at: entry.expires_at,
      version: entry.version,
      created_at: entry.created_at,
      updated_at: entry.updated_at,
      lease_id: entry.lease_id,
      revision: revision
    }
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

  defp normalize_put_options(opts) do
    with {:ok, opts} <- normalize_options_map(opts),
         {:ok, conditions} <- normalize_conditions(opts),
         {:ok, lease_id} <- normalize_optional_id(condition_value(opts, :lease_id)) do
      {:ok, %{conditions: conditions, lease_id: lease_id}}
    else
      {:error, :invalid_lease} -> {:error, :invalid_lease}
      _ -> {:error, :invalid_conditions}
    end
  end

  defp normalize_options_map(nil), do: {:ok, %{}}
  defp normalize_options_map(opts) when is_map(opts), do: {:ok, opts}

  defp normalize_options_map(opts) when is_list(opts) do
    try do
      {:ok, Map.new(opts)}
    rescue
      _ -> {:error, :invalid_options}
    end
  end

  defp normalize_options_map(_), do: {:error, :invalid_options}

  defp normalize_lease_ttl(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_lease_ttl(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> {:ok, int}
      _ -> {:error, :invalid_lease}
    end
  end

  defp normalize_lease_ttl(_), do: {:error, :invalid_lease}

  defp normalize_lease_create_id(opts) do
    with {:ok, opts} <- normalize_options_map(opts) do
      normalize_optional_id(condition_value(opts, :id))
    end
  end

  defp normalize_optional_id(nil), do: {:ok, nil}
  defp normalize_optional_id(value) when is_binary(value) and value != "", do: {:ok, value}
  defp normalize_optional_id(_), do: {:error, :invalid_lease}

  defp normalize_required_id(value) when is_binary(value) and value != "", do: {:ok, value}
  defp normalize_required_id(_), do: {:error, :invalid_lease}

  defp normalize_lock_holder(nil), do: {:ok, nil}
  defp normalize_lock_holder(value) when is_binary(value), do: {:ok, value}
  defp normalize_lock_holder(_), do: {:error, :invalid_lock}

  defp normalize_conditions(nil), do: {:ok, default_conditions()}

  defp normalize_conditions(opts) when is_list(opts) do
    try do
      opts
      |> Map.new()
      |> normalize_conditions()
    rescue
      _ -> {:error, :invalid_conditions}
    end
  end

  defp normalize_conditions(opts) when is_map(opts) do
    with {:ok, if_absent} <- normalize_bool(condition_value(opts, :if_absent)),
         {:ok, if_present} <- normalize_bool(condition_value(opts, :if_present)),
         {:ok, if_version} <- normalize_version(condition_value(opts, :if_version)) do
      cond do
        if_absent and if_present ->
          {:error, :invalid_conditions}

        if_absent and not is_nil(if_version) ->
          {:error, :invalid_conditions}

        true ->
          {:ok,
           %{
             if_absent: if_absent,
             if_present: if_present,
             if_version: if_version
           }}
      end
    end
  end

  defp normalize_conditions(_), do: {:error, :invalid_conditions}

  defp default_conditions do
    %{if_absent: false, if_present: false, if_version: nil}
  end

  defp condition_value(opts, key) do
    if Map.has_key?(opts, key) do
      Map.get(opts, key)
    else
      Map.get(opts, to_string(key))
    end
  end

  defp normalize_bool(nil), do: {:ok, false}
  defp normalize_bool(value) when is_boolean(value), do: {:ok, value}

  defp normalize_bool(value) when is_binary(value) do
    case String.downcase(value) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, :invalid_conditions}
    end
  end

  defp normalize_bool(_), do: {:error, :invalid_conditions}

  defp normalize_version(nil), do: {:ok, nil}
  defp normalize_version(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_version(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> {:ok, int}
      _ -> {:error, :invalid_conditions}
    end
  end

  defp normalize_version(_), do: {:error, :invalid_conditions}

  defp normalize_since_revision(nil), do: {:ok, nil}
  defp normalize_since_revision(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp normalize_since_revision(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, :invalid_watch}
    end
  end

  defp normalize_since_revision(_), do: {:error, :invalid_watch}

  defp conditions_met?(current, conditions) do
    absent_ok = not conditions.if_absent or is_nil(current)
    present_ok = not conditions.if_present or not is_nil(current)

    version_ok =
      is_nil(conditions.if_version) ||
        (not is_nil(current) and current.version == conditions.if_version)

    absent_ok and present_ok and version_ok
  end

  defp normalize_txn(compare, success, failure) do
    with {:ok, compare} <- normalize_compare_list(compare),
         {:ok, success} <- normalize_txn_ops(success),
         {:ok, failure} <- normalize_txn_ops(failure) do
      {:ok, %{compare: compare, success: success, failure: failure}}
    else
      _ -> {:error, :invalid_transaction}
    end
  end

  defp normalize_compare_list(compares) do
    normalize_list(compares, &normalize_compare/1)
  end

  defp normalize_txn_ops(ops) do
    normalize_list(ops, &normalize_txn_op/1)
  end

  defp normalize_list(items, normalizer) do
    items
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      case normalizer.(item) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_compare(compare) when is_map(compare) do
    key = condition_value(compare, :key)
    version = condition_value(compare, :version)
    exists = condition_value(compare, :exists)
    value = condition_value(compare, :value)

    with true <- is_binary(key),
         {:ok, version} <- normalize_version(version),
         {:ok, exists} <- normalize_optional_bool(exists),
         {:ok, value} <- normalize_optional_value(value),
         true <- not (is_nil(version) and is_nil(exists) and is_nil(value)) do
      {:ok, %{key: key, version: version, exists: exists, value: value}}
    else
      _ -> {:error, :invalid_transaction}
    end
  end

  defp normalize_compare(_), do: {:error, :invalid_transaction}

  defp normalize_txn_op(op) when is_map(op) do
    case normalize_op_name(condition_value(op, :op)) do
      :put -> normalize_put_op(op)
      :delete -> normalize_key_op(op, :delete)
      :get -> normalize_key_op(op, :get)
      nil -> {:error, :invalid_transaction}
    end
  end

  defp normalize_txn_op(_), do: {:error, :invalid_transaction}

  defp normalize_op_name(op) when is_atom(op), do: normalize_op_name(Atom.to_string(op))

  defp normalize_op_name(op) when is_binary(op) do
    case String.downcase(op) do
      "put" -> :put
      "set" -> :put
      "delete" -> :delete
      "del" -> :delete
      "get" -> :get
      _ -> nil
    end
  end

  defp normalize_op_name(_), do: nil

  defp normalize_put_op(op) do
    key = condition_value(op, :key)
    value = condition_value(op, :value)

    if is_binary(key) and is_binary(value) do
      {:ok, %{op: :put, key: key, value: value, ttl_ms: condition_value(op, :ttl_ms)}}
    else
      {:error, :invalid_transaction}
    end
  end

  defp normalize_key_op(op, name) do
    key = condition_value(op, :key)

    if is_binary(key) do
      {:ok, %{op: name, key: key}}
    else
      {:error, :invalid_transaction}
    end
  end

  defp normalize_optional_bool(nil), do: {:ok, nil}

  defp normalize_optional_bool(value) do
    case normalize_bool(value) do
      {:ok, bool} -> {:ok, bool}
      {:error, _} -> {:error, :invalid_transaction}
    end
  end

  defp normalize_optional_value(nil), do: {:ok, nil}
  defp normalize_optional_value(value) when is_binary(value), do: {:ok, value}
  defp normalize_optional_value(_), do: {:error, :invalid_transaction}

  defp compare_met?(compare, now) do
    current = current_entry(compare.key, now)

    version_ok =
      is_nil(compare.version) ||
        (not is_nil(current) and current.version == compare.version)

    exists_ok =
      is_nil(compare.exists) ||
        not is_nil(current) == compare.exists

    value_ok =
      is_nil(compare.value) ||
        (not is_nil(current) and current.value == compare.value)

    version_ok and exists_ok and value_ok
  end

  defp apply_txn_ops(ops, state, now) do
    Enum.map_reduce(ops, state, fn op, state ->
      apply_txn_op(op, state, now)
    end)
  end

  defp apply_txn_op(%{op: :put} = op, state, now) do
    expires_at = normalize_ttl(op.ttl_ms, now)

    if expires_at == :expired do
      current = current_entry(op.key, now)
      state = delete_entry(state, op.key, current, now, "delete")
      {%{op: "put", key: op.key, deleted: not is_nil(current)}, state}
    else
      entry = next_entry(op.key, op.value, expires_at, current_entry(op.key, now), now)
      state = put_entry(state, entry, now)
      {Map.put(entry_result(entry), :op, "put"), state}
    end
  end

  defp apply_txn_op(%{op: :delete, key: key}, state, now) do
    current = current_entry(key, now)
    state = delete_entry(state, key, current, now, "delete")
    {%{op: "delete", key: key, deleted: not is_nil(current)}, state}
  end

  defp apply_txn_op(%{op: :get, key: key}, state, now) do
    case current_entry(key, now) do
      nil -> {%{op: "get", key: key, found: false, value: nil}, state}
      entry -> {entry |> entry_result() |> Map.put(:op, "get") |> Map.put(:found, true), state}
    end
  end

  defp entry_result(entry) do
    %{
      key: entry.key,
      value: entry.value,
      metadata: %{
        version: entry.version,
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        expires_at: entry.expires_at,
        lease_id: entry.lease_id
      }
    }
  end

  defp put_entry(state, entry, now) do
    revision = next_revision(state)

    insert_entry(entry)
    append_aof(state, set_record(entry, revision))

    state
    |> Map.put(:revision, revision)
    |> publish_event(put_event(entry, revision, now))
  end

  defp delete_entry(state, _key, nil, _now, _event_op), do: state

  defp delete_entry(state, key, current, now, event_op) do
    revision = next_revision(state)
    aof_op = if event_op == "expire", do: "expire", else: "del"

    delete_stored_entry(key)
    append_aof(state, %{op: aof_op, key: key, revision: revision})

    state
    |> Map.put(:revision, revision)
    |> publish_event(delete_event(event_op, key, current, revision, now))
  end

  defp next_revision(state), do: state.revision + 1

  defp put_event(entry, revision, now) do
    entry
    |> entry_result()
    |> Map.merge(%{
      op: "put",
      revision: revision,
      timestamp: now
    })
  end

  defp delete_event(op, key, current, revision, now) do
    %{
      op: op,
      key: key,
      value: nil,
      metadata: entry_result(current).metadata,
      revision: revision,
      timestamp: now
    }
  end

  defp lease(id, ttl_ms, created_at, expires_at, updated_at) do
    %{
      id: id,
      ttl_ms: ttl_ms,
      created_at: created_at,
      expires_at: expires_at,
      updated_at: updated_at
    }
  end

  defp lock(name, lease_id, holder, acquired_at, expires_at) do
    %{
      name: name,
      lease_id: lease_id,
      holder: holder,
      acquired_at: acquired_at,
      expires_at: expires_at
    }
  end

  defp lease_result(lease) do
    %{
      id: lease.id,
      ttl_ms: lease.ttl_ms,
      created_at: lease.created_at,
      updated_at: lease.updated_at,
      expires_at: lease.expires_at
    }
  end

  defp lock_result(lock) do
    %{
      name: lock.name,
      lease_id: lock.lease_id,
      holder: lock.holder,
      acquired_at: lock.acquired_at,
      expires_at: lock.expires_at
    }
  end

  defp lease_record(op, lease) do
    lease
    |> lease_result()
    |> Map.put(:op, op)
  end

  defp lock_record(op, lock) do
    lock
    |> lock_result()
    |> Map.put(:op, op)
  end

  defp write_expires_at(ttl_ms, nil, _state, now), do: {:ok, normalize_ttl(ttl_ms, now)}

  defp write_expires_at(_ttl_ms, lease_id, state, now) do
    case active_lease(state.leases, lease_id, now) do
      nil -> {:error, :lease_not_found}
      lease -> {:ok, lease.expires_at}
    end
  end

  defp active_lease(leases, lease_id, now) do
    case Map.get(leases, lease_id) do
      nil -> nil
      lease -> if expired?(lease.expires_at, now), do: nil, else: lease
    end
  end

  defp active_lock(state, name, now) do
    case Map.get(state.locks, name) do
      nil ->
        nil

      lock ->
        case active_lease(state.leases, lock.lease_id, now) do
          nil -> nil
          lease -> %{lock | expires_at: lease.expires_at}
        end
    end
  end

  defp unique_lease_id(leases) do
    id = "lease-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

    if Map.has_key?(leases, id) do
      unique_lease_id(leases)
    else
      id
    end
  end

  defp put_lease_state(state, lease) do
    %{state | leases: Map.put(state.leases, lease.id, lease)}
  end

  defp remove_lease_state(state, lease_id) do
    %{state | leases: Map.delete(state.leases, lease_id)}
  end

  defp put_lock_state(state, lock) do
    %{state | locks: Map.put(state.locks, lock.name, lock)}
  end

  defp remove_lock_state(state, name) do
    %{state | locks: Map.delete(state.locks, name)}
  end

  defp remove_locks_for_lease(state, lease_id) do
    locks =
      state.locks
      |> Enum.reject(fn {_name, lock} -> lock.lease_id == lease_id end)
      |> Map.new()

    %{state | locks: locks}
  end

  defp renew_attached_locks(state, lease) do
    locks =
      Map.new(state.locks, fn {name, lock} ->
        if lock.lease_id == lease.id do
          {name, %{lock | expires_at: lease.expires_at}}
        else
          {name, lock}
        end
      end)

    %{state | locks: locks}
  end

  defp renew_attached_entries(state, lease, now) do
    lease.id
    |> attached_entries(now)
    |> Enum.reduce(state, fn current, state ->
      entry = next_entry(current.key, current.value, lease.expires_at, current, now, lease.id)
      put_entry(state, entry, now)
    end)
  end

  defp revoke_lease(state, lease_id, now, lease_op, key_event_op) do
    entries = attached_entries(lease_id, now)

    state =
      state
      |> remove_lease_state(lease_id)
      |> remove_locks_for_lease(lease_id)
      |> append_metadata_aof(%{op: lease_op, id: lease_id})

    Enum.reduce(entries, state, fn current, state ->
      delete_entry(state, current.key, current, now, key_event_op)
    end)
  end

  defp drop_expired_lease_id(state, nil, _now), do: state

  defp drop_expired_lease_id(state, lease_id, now) do
    case Map.get(state.leases, lease_id) do
      nil ->
        state

      lease ->
        if expired?(lease.expires_at, now) do
          revoke_lease(state, lease_id, now, "lease_expire", "expire")
        else
          state
        end
    end
  end

  defp attached_entries(lease_id, now) do
    @table
    |> :ets.tab2list()
    |> Enum.flat_map(fn tuple ->
      case entry_from_tuple(tuple, now) do
        %{lease_id: ^lease_id} = entry -> [entry]
        _ -> []
      end
    end)
  end

  defp append_metadata_aof(state, record) do
    revision = next_revision(state)

    append_aof(state, Map.put(record, :revision, revision))
    %{state | revision: revision}
  end

  defp publish_event(state, event) do
    Enum.each(state.watchers, fn {ref, watcher} ->
      if matches_prefix?(event.key, watcher.prefix) do
        send(watcher.pid, {__MODULE__, :watch_event, ref, event})
      end
    end)

    %{
      state
      | watch_events: store_watch_event(state.watch_events, event, state.watch_history_limit)
    }
  end

  defp store_watch_event(events, event, limit) do
    events
    |> Kernel.++([event])
    |> trim_watch_events(limit)
  end

  defp trim_watch_events(events, limit) do
    count = length(events)

    if count > limit do
      Enum.drop(events, count - limit)
    else
      events
    end
  end

  defp watch_replay(_state, _prefix, nil), do: {:ok, []}

  defp watch_replay(state, prefix, since_revision) do
    oldest_available =
      case state.watch_events do
        [first | _] -> first.revision
        [] -> state.revision + 1
      end

    cond do
      since_revision > state.revision ->
        {:ok, []}

      since_revision < state.revision and since_revision < oldest_available - 1 ->
        {:error, :history_unavailable}

      true ->
        events =
          state.watch_events
          |> Enum.filter(&(&1.revision > since_revision))
          |> Enum.filter(&matches_prefix?(&1.key, prefix))

        {:ok, events}
    end
  end

  defp remove_watcher(state, ref) do
    case Map.pop(state.watchers, ref) do
      {nil, _watchers} ->
        state

      {watcher, watchers} ->
        Process.demonitor(watcher.monitor, [:flush])
        %{state | watchers: watchers}
    end
  end

  defp cleanup_expired(now, state) do
    state =
      state.leases
      |> Map.values()
      |> Enum.filter(&expired?(&1.expires_at, now))
      |> Enum.reduce(state, fn lease, state ->
        revoke_lease(state, lease.id, now, "lease_expire", "expire")
      end)

    @table
    |> :ets.tab2list()
    |> Enum.flat_map(fn tuple ->
      case entry_from_tuple(tuple, now) do
        nil -> []
        entry -> [entry]
      end
    end)
    |> Enum.filter(&expired?(&1.expires_at, now))
    |> Enum.reduce(state, fn current, state ->
      delete_entry(state, current.key, current, now, "expire")
    end)
    |> maybe_sync()
  end

  defp load_snapshot(path) do
    case File.read(path) do
      {:ok, content} ->
        now = now_ms()

        case Jason.decode(content) do
          {:ok, %{"data" => data} = payload} when is_map(data) ->
            if valid_snapshot_checksum?(payload) do
              load_entries(data, now)
              leases = load_leases(Map.get(payload, "leases"), now)
              locks = load_locks(Map.get(payload, "locks"), leases, now)

              %{
                checksum: Map.get(payload, "checksum"),
                created_at: Map.get(payload, "created_at"),
                store_revision: snapshot_store_revision(payload, 0),
                leases: leases,
                locks: locks
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
            version = normalize_stored_version(Map.get(map, "version"))
            created_at = normalize_stored_timestamp(Map.get(map, "created_at"), now)
            updated_at = normalize_stored_timestamp(Map.get(map, "updated_at"), created_at)
            lease_id = normalize_stored_id(Map.get(map, "lease_id"))

            insert_entry(entry(key, value, expires_at, version, created_at, updated_at, lease_id))
          end

        value when is_binary(value) ->
          insert_entry(entry(key, value, nil, 1, now, now))

        _ ->
          :ok
      end
    end)
  end

  defp replay_aof(path, start_revision, history_limit, leases, locks) do
    initial = %{revision: start_revision, events: [], leases: leases, locks: locks}

    case File.open(path, [:read]) do
      {:ok, io} ->
        now = now_ms()

        result =
          io
          |> IO.stream(:line)
          |> Stream.map(&String.trim/1)
          |> Stream.reject(&(&1 == ""))
          |> Enum.reduce(initial, fn line, acc ->
            case Jason.decode(line) do
              {:ok, record} ->
                if valid_aof_checksum?(record) do
                  replay_aof_record(record, acc, now, history_limit)
                else
                  Logger.warning("Skipping AOF entry with invalid checksum")
                  acc
                end

              {:error, reason} ->
                Logger.warning("Skipping invalid AOF line: #{inspect(reason)}")
                acc
            end
          end)
          |> prune_loaded_state(now)

        File.close(io)
        result

      {:error, :enoent} ->
        prune_loaded_state(initial, now_ms())

      {:error, reason} ->
        Logger.warning("Failed to read AOF: #{inspect(reason)}")
        prune_loaded_state(initial, now_ms())
    end
  end

  defp load_leases(nil, _now), do: %{}

  defp load_leases(leases, now) when is_map(leases) do
    leases
    |> Enum.reduce(%{}, fn {_id, value}, acc ->
      case normalize_stored_lease(value, now) do
        nil -> acc
        lease -> Map.put(acc, lease.id, lease)
      end
    end)
  end

  defp load_leases(_leases, _now), do: %{}

  defp load_locks(nil, _leases, _now), do: %{}

  defp load_locks(locks, leases, now) when is_map(locks) do
    locks
    |> Enum.reduce(%{}, fn {_name, value}, acc ->
      case normalize_stored_lock(value, leases, now) do
        nil -> acc
        lock -> Map.put(acc, lock.name, lock)
      end
    end)
  end

  defp load_locks(_locks, _leases, _now), do: %{}

  # Cursor-based key collection helpers

  defp normalize_range_options(opts) do
    with {:ok, opts} <- normalize_options_map(opts),
         {:ok, cursor} <- normalize_range_cursor(condition_value(opts, :cursor)),
         {:ok, start} <- normalize_range_start(condition_value(opts, :start)),
         {:ok, include_values} <- normalize_range_bool(condition_value(opts, :include_values)),
         {:ok, include_metadata} <- normalize_range_bool(condition_value(opts, :include_metadata)) do
      {:ok,
       %{
         cursor: cursor,
         start: start,
         include_values: include_values,
         include_metadata: include_metadata
       }}
    else
      _ -> {:error, :invalid_args}
    end
  end

  defp normalize_range_cursor(nil), do: {:ok, nil}
  defp normalize_range_cursor(value) when is_binary(value), do: {:ok, value}
  defp normalize_range_cursor(_value), do: {:error, :invalid_args}

  defp normalize_range_start(nil), do: {:ok, nil}
  defp normalize_range_start(value) when is_binary(value), do: {:ok, value}
  defp normalize_range_start(_value), do: {:error, :invalid_args}

  defp normalize_range_bool(nil), do: {:ok, false}
  defp normalize_range_bool(value) when is_boolean(value), do: {:ok, value}

  defp normalize_range_bool(value) when is_binary(value) do
    case String.downcase(value) do
      "true" -> {:ok, true}
      "1" -> {:ok, true}
      "false" -> {:ok, false}
      "0" -> {:ok, false}
      _ -> {:error, :invalid_args}
    end
  end

  defp normalize_range_bool(_value), do: {:error, :invalid_args}

  defp decode_cursor_key(nil, _prefix), do: {:ok, nil}
  defp decode_cursor_key("", _prefix), do: {:ok, nil}

  defp decode_cursor_key(cursor, expected_prefix) do
    case Toska.Cursor.decode(cursor) do
      {:ok, {key, ^expected_prefix}} -> {:ok, key}
      {:ok, {_key, _different_prefix}} -> {:error, :invalid_cursor}
      {:error, _} -> {:error, :invalid_cursor}
    end
  end

  defp range_start(_prefix, _start, cursor_key) when is_binary(cursor_key) do
    {:exclusive, cursor_key}
  end

  defp range_start(prefix, start, nil) do
    lower_bound =
      cond do
        is_nil(start) and prefix == "" -> nil
        is_nil(start) -> prefix
        prefix == "" -> start
        start > prefix -> start
        true -> prefix
      end

    {:inclusive, lower_bound}
  end

  defp collect_range_entries(prefix, start, limit, now) do
    first_key =
      case start do
        {:exclusive, key} -> :ets.next(@key_index_table, key)
        {:inclusive, nil} -> :ets.first(@key_index_table)
        {:inclusive, key} -> first_key_at_or_after(key)
      end

    entries = collect_range_from(first_key, prefix, limit + 1, now, [])

    if length(entries) > limit do
      {Enum.take(entries, limit), true}
    else
      {entries, false}
    end
  end

  defp first_key_at_or_after(key) do
    case :ets.lookup(@key_index_table, key) do
      [{^key}] -> key
      _ -> :ets.next(@key_index_table, key)
    end
  end

  defp collect_range_from(:"$end_of_table", _prefix, _remaining, _now, acc) do
    Enum.reverse(acc)
  end

  defp collect_range_from(_key, _prefix, 0, _now, acc) do
    Enum.reverse(acc)
  end

  defp collect_range_from(key, prefix, remaining, now, acc) do
    if not matches_prefix?(key, prefix) do
      Enum.reverse(acc)
    else
      next_key = :ets.next(@key_index_table, key)

      case current_entry(key, now) do
        nil -> collect_range_from(next_key, prefix, remaining, now, acc)
        entry -> collect_range_from(next_key, prefix, remaining - 1, now, [entry | acc])
      end
    end
  end

  defp range_item(entry, opts) do
    %{key: entry.key}
    |> maybe_put_range_value(entry, opts.include_values)
    |> maybe_put_range_metadata(entry, opts.include_metadata)
  end

  defp maybe_put_range_value(item, _entry, false), do: item
  defp maybe_put_range_value(item, entry, true), do: Map.put(item, :value, entry.value)

  defp maybe_put_range_metadata(item, _entry, false), do: item

  defp maybe_put_range_metadata(item, entry, true) do
    Map.put(item, :metadata, entry_result(entry).metadata)
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

  defp write_snapshot(state) do
    now = now_ms()

    data =
      :ets.tab2list(@table)
      |> Enum.reduce(%{}, fn tuple, acc ->
        case entry_from_tuple(tuple, now) do
          nil ->
            acc

          entry ->
            if expired?(entry.expires_at, now) do
              acc
            else
              Map.put(acc, entry.key, %{
                "value" => entry.value,
                "expires_at" => entry.expires_at,
                "version" => entry.version,
                "created_at" => entry.created_at,
                "updated_at" => entry.updated_at,
                "lease_id" => entry.lease_id
              })
            end
        end
      end)

    checksum = snapshot_checksum(data)

    payload = %{
      "version" => @snapshot_version,
      "created_at" => now,
      "store_revision" => state.revision,
      "checksum" => checksum,
      "data" => data,
      "leases" => snapshot_leases(state.leases, now),
      "locks" => snapshot_locks(state.locks, state.leases, now)
    }

    tmp_path = state.snapshot_path <> ".tmp"

    with {:ok, json} <- Jason.encode(payload, pretty: true),
         :ok <- File.write(tmp_path, json),
         :ok <- File.rename(tmp_path, state.snapshot_path) do
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
      case write_snapshot(state) do
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
      compaction_aof_bytes: @default_compaction_aof_bytes,
      watch_history_limit: @default_watch_history_limit
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
        parse_int(config["compaction_aof_bytes"], defaults.compaction_aof_bytes),
      watch_history_limit: parse_int(config["watch_history_limit"], defaults.watch_history_limit)
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

  defp normalize_stored_version(value) when is_integer(value) and value > 0, do: value

  defp normalize_stored_version(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> 1
    end
  end

  defp normalize_stored_version(_), do: 1

  defp normalize_stored_timestamp(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_stored_timestamp(_value, default), do: default

  defp normalize_stored_id(value) when is_binary(value) and value != "", do: value
  defp normalize_stored_id(_), do: nil

  defp normalize_stored_holder(nil), do: nil
  defp normalize_stored_holder(value) when is_binary(value), do: value
  defp normalize_stored_holder(_), do: nil

  defp normalize_stored_positive_int(value) when is_integer(value) and value > 0, do: value

  defp normalize_stored_positive_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> nil
    end
  end

  defp normalize_stored_positive_int(_), do: nil

  defp normalize_stored_lease(value, now) when is_map(value) do
    id = normalize_stored_id(condition_value(value, :id))
    ttl_ms = normalize_stored_positive_int(condition_value(value, :ttl_ms))
    created_at = normalize_stored_timestamp(condition_value(value, :created_at), now)
    expires_at = normalize_stored_timestamp(condition_value(value, :expires_at), nil)
    updated_at = normalize_stored_timestamp(condition_value(value, :updated_at), created_at)

    cond do
      is_nil(id) or is_nil(ttl_ms) or is_nil(expires_at) ->
        nil

      expired?(expires_at, now) ->
        nil

      true ->
        lease(id, ttl_ms, created_at, expires_at, updated_at)
    end
  end

  defp normalize_stored_lease(_value, _now), do: nil

  defp normalize_stored_lock(value, leases, now) when is_map(value) do
    name = normalize_stored_id(condition_value(value, :name))
    lease_id = normalize_stored_id(condition_value(value, :lease_id))
    holder = normalize_stored_holder(condition_value(value, :holder))
    acquired_at = normalize_stored_timestamp(condition_value(value, :acquired_at), now)

    with name when is_binary(name) <- name,
         lease_id when is_binary(lease_id) <- lease_id,
         lease when not is_nil(lease) <- active_lease(leases, lease_id, now) do
      lock(name, lease_id, holder, acquired_at, lease.expires_at)
    else
      _ -> nil
    end
  end

  defp normalize_stored_lock(_value, _leases, _now), do: nil

  defp snapshot_leases(leases, now) do
    leases
    |> Map.values()
    |> Enum.reject(&expired?(&1.expires_at, now))
    |> Map.new(fn lease ->
      {lease.id,
       %{
         "id" => lease.id,
         "ttl_ms" => lease.ttl_ms,
         "created_at" => lease.created_at,
         "updated_at" => lease.updated_at,
         "expires_at" => lease.expires_at
       }}
    end)
  end

  defp snapshot_locks(locks, leases, now) do
    locks
    |> Enum.reduce(%{}, fn {name, lock}, acc ->
      case active_lease(leases, lock.lease_id, now) do
        nil ->
          acc

        lease ->
          Map.put(acc, name, %{
            "name" => name,
            "lease_id" => lock.lease_id,
            "holder" => lock.holder,
            "acquired_at" => lock.acquired_at,
            "expires_at" => lease.expires_at
          })
      end
    end)
  end

  defp active_lease_count(leases) do
    now = now_ms()

    Enum.count(leases, fn {_id, lease} ->
      not expired?(lease.expires_at, now)
    end)
  end

  defp active_lock_count(locks, leases) do
    now = now_ms()

    Enum.count(locks, fn {_name, lock} ->
      not is_nil(active_lease(leases, lock.lease_id, now))
    end)
  end

  defp prune_loaded_state(state, now) do
    leases =
      state.leases
      |> Enum.reject(fn {_id, lease} -> expired?(lease.expires_at, now) end)
      |> Map.new()

    locks =
      state.locks
      |> Enum.reject(fn {_name, lock} -> is_nil(active_lease(leases, lock.lease_id, now)) end)
      |> Map.new()

    :ets.tab2list(@table)
    |> Enum.each(fn tuple ->
      case entry_from_tuple(tuple, now) do
        %{lease_id: lease_id, key: key} when not is_nil(lease_id) ->
          if is_nil(active_lease(leases, lease_id, now)) do
            delete_stored_entry(key)
          end

        _ ->
          :ok
      end
    end)

    %{state | leases: leases, locks: locks}
  end

  defp snapshot_store_revision(payload, default) do
    normalize_store_revision(Map.get(payload, "store_revision"), default)
  end

  defp normalize_store_revision(value, _default) when is_integer(value) and value >= 0, do: value

  defp normalize_store_revision(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> int
      _ -> default
    end
  end

  defp normalize_store_revision(_value, default), do: default

  defp replay_aof_record(record, acc, now, history_limit) do
    revision = record_revision(record, acc.revision + 1)
    record = Map.put(record, "revision", revision)

    apply_aof_record(record, now)
    acc = apply_state_record(acc, record, now)

    events =
      case event_from_aof_record(record, now) do
        nil -> acc.events
        event -> store_watch_event(acc.events, event, history_limit)
      end

    %{acc | revision: max(acc.revision, revision), events: events}
  end

  defp apply_replicated_record(record, state, now) do
    revision = record_revision(record, state.revision + 1)
    record = Map.put(record, "revision", revision)

    apply_aof_record(record, now)
    append_aof(state, record)

    state =
      state
      |> apply_state_record(record, now)
      |> Map.put(:revision, max(state.revision, revision))

    case event_from_aof_record(record, now) do
      nil -> state
      event -> publish_event(state, event)
    end
  end

  defp record_revision(record, default) do
    record
    |> Map.get("revision", Map.get(record, :revision))
    |> normalize_store_revision(default)
  end

  defp apply_state_record(state, record, now) do
    case condition_value(record, :op) do
      "lease_create" ->
        case normalize_stored_lease(record, now) do
          nil -> state
          lease -> put_lease_state(state, lease)
        end

      "lease_keepalive" ->
        case normalize_stored_lease(record, now) do
          nil ->
            state

          lease ->
            state
            |> put_lease_state(lease)
            |> renew_attached_locks(lease)
        end

      op when op in ["lease_delete", "lease_expire"] ->
        case normalize_stored_id(condition_value(record, :id)) do
          nil ->
            state

          lease_id ->
            state
            |> remove_lease_state(lease_id)
            |> remove_locks_for_lease(lease_id)
        end

      "lock_acquire" ->
        case normalize_stored_lock(record, state.leases, now) do
          nil -> state
          lock -> put_lock_state(state, lock)
        end

      "lock_release" ->
        case normalize_stored_id(condition_value(record, :name)) do
          nil -> state
          name -> remove_lock_state(state, name)
        end

      _ ->
        state
    end
  end

  defp apply_aof_record(%{"op" => "set", "key" => key, "value" => value} = record, now) do
    expires_at = Map.get(record, "expires_at")

    unless expired?(expires_at, now) do
      entry =
        if Map.has_key?(record, "version") do
          entry(
            key,
            value,
            expires_at,
            normalize_stored_version(Map.get(record, "version")),
            normalize_stored_timestamp(Map.get(record, "created_at"), now),
            normalize_stored_timestamp(Map.get(record, "updated_at"), now),
            normalize_stored_id(Map.get(record, "lease_id"))
          )
        else
          next_entry(key, value, expires_at, current_entry(key, now), now)
        end

      insert_entry(entry)
    end
  end

  defp apply_aof_record(%{"op" => "del", "key" => key}, _now) do
    delete_stored_entry(key)
  end

  defp apply_aof_record(%{"op" => "expire", "key" => key}, _now) do
    delete_stored_entry(key)
  end

  defp apply_aof_record(_record, _now), do: :ok

  defp event_from_aof_record(%{"op" => "set", "key" => _key, "value" => _value} = record, now) do
    expires_at = Map.get(record, "expires_at")

    if expired?(expires_at, now) do
      nil
    else
      revision = record_revision(record, 0)

      entry =
        entry(
          Map.get(record, "key"),
          Map.get(record, "value"),
          expires_at,
          normalize_stored_version(Map.get(record, "version")),
          normalize_stored_timestamp(Map.get(record, "created_at"), now),
          normalize_stored_timestamp(Map.get(record, "updated_at"), now),
          normalize_stored_id(Map.get(record, "lease_id"))
        )

      put_event(entry, revision, now)
    end
  end

  defp event_from_aof_record(%{"op" => op, "key" => key} = record, now)
       when op in ["del", "expire"] do
    event_op = if op == "expire", do: "expire", else: "delete"
    revision = record_revision(record, 0)

    %{
      op: event_op,
      key: key,
      value: nil,
      metadata: nil,
      revision: revision,
      timestamp: now
    }
  end

  defp event_from_aof_record(_record, _now), do: nil

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
