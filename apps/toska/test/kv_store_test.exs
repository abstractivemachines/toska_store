defmodule Toska.KVStoreTest do
  use ExUnit.Case, async: false

  setup do
    original_data_dir = System.get_env("TOSKA_DATA_DIR")
    tmp_dir = Path.join([System.tmp_dir!(), "toska_kv_#{System.unique_integer([:positive])}"])

    File.mkdir_p!(tmp_dir)
    System.put_env("TOSKA_DATA_DIR", tmp_dir)

    stop_store()
    start_store()

    on_exit(fn ->
      stop_store()

      case original_data_dir do
        nil -> System.delete_env("TOSKA_DATA_DIR")
        value -> System.put_env("TOSKA_DATA_DIR", value)
      end

      File.rm_rf(tmp_dir)
    end)

    :ok
  end

  test "put and get return stored value" do
    assert :ok = Toska.KVStore.put("alpha", "1")
    assert {:ok, "1"} = Toska.KVStore.get("alpha")
  end

  test "get_entry returns metadata and increments versions on update" do
    assert :ok = Toska.KVStore.put("meta", "1")
    assert {:ok, first} = Toska.KVStore.get_entry("meta")
    assert first.value == "1"
    assert first.version == 1
    assert is_integer(first.created_at)
    assert is_integer(first.updated_at)
    assert is_nil(first.expires_at)

    :timer.sleep(2)
    assert :ok = Toska.KVStore.put("meta", "2")
    assert {:ok, second} = Toska.KVStore.get_entry("meta")
    assert second.value == "2"
    assert second.version == 2
    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at
  end

  test "conditional puts enforce absent present and version checks" do
    assert :ok = Toska.KVStore.put("nil-conditions", "1", nil, nil)
    assert :ok = Toska.KVStore.put("cas", "1", nil, if_absent: true)
    assert {:error, :condition_failed} = Toska.KVStore.put("cas", "again", nil, if_absent: true)

    assert {:error, :condition_failed} = Toska.KVStore.put("missing", "1", nil, if_present: true)

    assert :ok = Toska.KVStore.put("cas", "2", nil, if_version: 1)
    assert {:ok, entry} = Toska.KVStore.get_entry("cas")
    assert entry.value == "2"
    assert entry.version == 2

    assert {:error, :condition_failed} = Toska.KVStore.put("cas", "stale", nil, if_version: 1)
    assert {:error, :invalid_conditions} = Toska.KVStore.put("cas", "bad", nil, ["bad"])

    assert {:error, :invalid_conditions} =
             Toska.KVStore.put("cas", "bad", nil, if_absent: true, if_present: true)
  end

  test "conditional deletes enforce version checks" do
    assert :ok = Toska.KVStore.put("delete-cas", "1")
    assert {:error, :condition_failed} = Toska.KVStore.delete("delete-cas", if_version: 2)
    assert {:ok, "1"} = Toska.KVStore.get("delete-cas")

    assert :ok = Toska.KVStore.delete("delete-cas", if_version: 1)
    assert {:error, :not_found} = Toska.KVStore.get("delete-cas")
  end

  test "txn applies success operations when compares pass" do
    assert :ok = Toska.KVStore.put("txn:item", "old")

    assert {:ok, result} =
             Toska.KVStore.txn(
               [%{key: "txn:item", version: 1}],
               [
                 %{op: "put", key: "txn:item", value: "new"},
                 %{op: "put", key: "txn:created", value: "yes"},
                 %{op: "get", key: "txn:item"}
               ],
               [%{op: "put", key: "txn:failed", value: "no"}]
             )

    assert result.succeeded == true
    assert Enum.map(result.responses, & &1.op) == ["put", "put", "get"]
    assert {:ok, item} = Toska.KVStore.get_entry("txn:item")
    assert item.value == "new"
    assert item.version == 2
    assert {:ok, "yes"} = Toska.KVStore.get("txn:created")
    assert {:error, :not_found} = Toska.KVStore.get("txn:failed")
  end

  test "txn applies failure operations when compares fail" do
    assert :ok = Toska.KVStore.put("txn:guard", "actual")

    assert {:ok, result} =
             Toska.KVStore.txn(
               [%{key: "txn:guard", exists: true, value: "expected"}],
               [%{op: "put", key: "txn:success", value: "yes"}],
               [
                 %{op: "put", key: "txn:fallback", value: "used"},
                 %{op: "delete", key: "txn:success"}
               ]
             )

    assert result.succeeded == false
    assert Enum.map(result.responses, & &1.op) == ["put", "delete"]
    assert {:ok, "used"} = Toska.KVStore.get("txn:fallback")
    assert {:error, :not_found} = Toska.KVStore.get("txn:success")
  end

  test "txn validates compare and operation payloads" do
    assert {:error, :invalid_transaction} = Toska.KVStore.txn([%{key: "bad"}], [], [])
    assert {:error, :invalid_transaction} = Toska.KVStore.txn([], [%{op: "put"}], [])
    assert {:error, :invalid_transaction} = Toska.KVStore.txn("bad", [], [])
  end

  test "watch replays matching events after a revision" do
    assert :ok = Toska.KVStore.put("watch:a", "1")
    assert :ok = Toska.KVStore.put("other:a", "skip")
    assert :ok = Toska.KVStore.put("watch:b", "2")

    assert {:ok, watch} = Toska.KVStore.watch("watch:", 0)

    assert watch.current_revision == 3
    assert Enum.map(watch.events, & &1.op) == ["put", "put"]
    assert Enum.map(watch.events, & &1.key) == ["watch:a", "watch:b"]
    assert Enum.map(watch.events, & &1.revision) == [1, 3]
    assert :ok = Toska.KVStore.unwatch(watch.ref)
  end

  test "watch subscribers receive live put delete and expire events" do
    assert {:ok, watch} = Toska.KVStore.watch("live:", nil)

    assert :ok = Toska.KVStore.put("live:key", "1")
    assert_receive {Toska.KVStore, :watch_event, ref, put_event}, 100
    assert ref == watch.ref
    assert put_event.op == "put"
    assert put_event.key == "live:key"
    assert put_event.value == "1"
    assert put_event.revision == 1

    assert :ok = Toska.KVStore.delete("live:key")
    assert_receive {Toska.KVStore, :watch_event, ^ref, delete_event}, 100
    assert delete_event.op == "delete"
    assert delete_event.key == "live:key"
    assert delete_event.revision == 2

    assert :ok = Toska.KVStore.put("live:ttl", "gone", 5)
    assert_receive {Toska.KVStore, :watch_event, ^ref, ttl_put_event}, 100
    assert ttl_put_event.op == "put"
    :timer.sleep(20)
    assert {:error, :not_found} = Toska.KVStore.get("live:ttl")
    :timer.sleep(1100)
    assert_receive {Toska.KVStore, :watch_event, ^ref, expire_event}, 1500
    assert expire_event.op == "expire"
    assert expire_event.key == "live:ttl"

    assert :ok = Toska.KVStore.unwatch(ref)
  end

  test "watch history is rebuilt from AOF on restart" do
    assert :ok = Toska.KVStore.put("replay:a", "1")
    assert :ok = Toska.KVStore.delete("replay:a")

    stop_store()
    start_store()

    assert {:ok, watch} = Toska.KVStore.watch("replay:", 0)
    assert Enum.map(watch.events, & &1.op) == ["put", "delete"]
    assert Enum.map(watch.events, & &1.revision) == [1, 2]
    assert watch.current_revision == 2
    assert :ok = Toska.KVStore.unwatch(watch.ref)
  end

  test "leases attach keys and keepalive extends leased key expiration" do
    assert {:ok, lease} = Toska.KVStore.create_lease(100, id: "lease-key")
    assert :ok = Toska.KVStore.put("leased:key", "value", nil, lease_id: lease.id)

    assert {:ok, entry} = Toska.KVStore.get_entry("leased:key")
    assert entry.lease_id == lease.id
    assert entry.expires_at == lease.expires_at
    assert entry.version == 1

    :timer.sleep(2)
    assert {:ok, renewed} = Toska.KVStore.keepalive_lease(lease.id)
    assert renewed.expires_at > lease.expires_at

    assert {:ok, renewed_entry} = Toska.KVStore.get_entry("leased:key")
    assert renewed_entry.lease_id == lease.id
    assert renewed_entry.expires_at == renewed.expires_at
    assert renewed_entry.version == 2
  end

  test "deleting a lease removes attached keys and locks" do
    assert {:ok, lease} = Toska.KVStore.create_lease(5_000, id: "lease-delete")
    assert :ok = Toska.KVStore.put("leased:delete", "value", nil, lease_id: lease.id)
    assert {:ok, lock} = Toska.KVStore.acquire_lock("cleanup-lock", lease.id, "worker-1")
    assert lock.lease_id == lease.id

    assert :ok = Toska.KVStore.delete_lease(lease.id)
    assert {:error, :not_found} = Toska.KVStore.get("leased:delete")
    assert {:error, :lock_not_found} = Toska.KVStore.release_lock("cleanup-lock", lease.id)
  end

  test "locks require an active lease and enforce ownership on release" do
    assert {:ok, owner} = Toska.KVStore.create_lease(5_000, id: "lock-owner")
    assert {:ok, contender} = Toska.KVStore.create_lease(5_000, id: "lock-contender")

    assert {:ok, lock} = Toska.KVStore.acquire_lock("job:lock", owner.id, "owner")
    assert lock.name == "job:lock"
    assert lock.lease_id == owner.id
    assert {:error, :lock_held} = Toska.KVStore.acquire_lock("job:lock", contender.id, nil)
    assert {:error, :lock_owner_mismatch} = Toska.KVStore.release_lock("job:lock", contender.id)
    assert :ok = Toska.KVStore.release_lock("job:lock", owner.id)
    assert {:ok, next_lock} = Toska.KVStore.acquire_lock("job:lock", contender.id, nil)
    assert next_lock.lease_id == contender.id
  end

  test "expired leases release keys and locks" do
    assert {:ok, lease} = Toska.KVStore.create_lease(5, id: "lease-expire")
    assert :ok = Toska.KVStore.put("leased:expire", "value", nil, lease_id: lease.id)
    assert {:ok, _lock} = Toska.KVStore.acquire_lock("expire-lock", lease.id, nil)

    :timer.sleep(20)
    assert {:error, :not_found} = Toska.KVStore.get("leased:expire")

    assert {:ok, next_lease} = Toska.KVStore.create_lease(5_000, id: "lease-expire-next")
    assert {:ok, lock} = Toska.KVStore.acquire_lock("expire-lock", next_lease.id, nil)
    assert lock.lease_id == next_lease.id
  end

  test "aof replay restores active leases locks and leased keys" do
    assert {:ok, lease} = Toska.KVStore.create_lease(5_000, id: "lease-persist")
    assert :ok = Toska.KVStore.put("leased:persist", "value", nil, lease_id: lease.id)
    assert {:ok, _lock} = Toska.KVStore.acquire_lock("persist-lock", lease.id, nil)

    stop_store()
    start_store()

    assert {:ok, entry} = Toska.KVStore.get_entry("leased:persist")
    assert entry.value == "value"
    assert entry.lease_id == lease.id

    assert {:ok, contender} = Toska.KVStore.create_lease(5_000, id: "lease-persist-contender")
    assert {:error, :lock_held} = Toska.KVStore.acquire_lock("persist-lock", contender.id, nil)
  end

  test "ttl expires keys" do
    assert :ok = Toska.KVStore.put("temp", "value", 10)
    :timer.sleep(20)
    assert {:error, :not_found} = Toska.KVStore.get("temp")
  end

  test "aof replay restores values on restart" do
    assert :ok = Toska.KVStore.put("persist", "yes")
    assert :ok = Toska.KVStore.put("persist", "yes2")
    stop_store()
    start_store()
    assert {:ok, entry} = Toska.KVStore.get_entry("persist")
    assert entry.value == "yes2"
    assert entry.version == 2
  end

  test "invalid snapshot checksum skips load" do
    stop_store()

    snapshot_path = Path.join(System.get_env("TOSKA_DATA_DIR"), "toska_snapshot.json")

    payload = %{
      "version" => 1,
      "created_at" => System.system_time(:millisecond),
      "checksum" => "bad",
      "data" => %{"ghost" => %{"value" => "1", "expires_at" => nil}}
    }

    File.write!(snapshot_path, Jason.encode!(payload))

    start_store()
    assert {:error, :not_found} = Toska.KVStore.get("ghost")
  end

  test "invalid AOF checksum is ignored" do
    stop_store()

    aof_path = Path.join(System.get_env("TOSKA_DATA_DIR"), "toska.aof")

    record = %{
      "v" => 1,
      "op" => "set",
      "key" => "bad",
      "value" => "1",
      "checksum" => "bad"
    }

    File.write!(aof_path, Jason.encode!(record) <> "\n")

    start_store()
    assert {:error, :not_found} = Toska.KVStore.get("bad")
  end

  test "apply_replication applies AOF records" do
    record = %{"v" => 1, "op" => "set", "key" => "rep", "value" => "ok"}
    record = Map.put(record, "checksum", checksum(record))

    assert :ok = Toska.KVStore.apply_replication([record])
    assert {:ok, "ok"} = Toska.KVStore.get("rep")
    assert {:ok, entry} = Toska.KVStore.get_entry("rep")
    assert entry.version == 1
  end

  test "apply_replication preserves metadata when present" do
    record = %{
      "v" => 1,
      "op" => "set",
      "key" => "rep-meta",
      "value" => "ok",
      "version" => 7,
      "created_at" => 100,
      "updated_at" => 200
    }

    record = Map.put(record, "checksum", checksum(record))

    assert :ok = Toska.KVStore.apply_replication([record])
    assert {:ok, entry} = Toska.KVStore.get_entry("rep-meta")
    assert entry.value == "ok"
    assert entry.version == 7
    assert entry.created_at == 100
    assert entry.updated_at == 200
  end

  test "apply_replication persists checksums without breaking replay" do
    record = %{"v" => 1, "op" => "set", "key" => "repersist", "value" => "ok"}
    record = Map.put(record, "checksum", checksum(record))

    assert :ok = Toska.KVStore.apply_replication([record])
    assert {:ok, "ok"} = Toska.KVStore.get("repersist")

    stop_store()
    start_store()

    assert {:ok, "ok"} = Toska.KVStore.get("repersist")
  end

  test "apply_replication skips invalid checksum records" do
    record = %{"v" => 1, "op" => "set", "key" => "bad", "value" => "1", "checksum" => "bad"}
    assert :ok = Toska.KVStore.apply_replication(record)
    assert {:error, :not_found} = Toska.KVStore.get("bad")
  end

  test "invalid inputs return errors" do
    assert {:error, :invalid_key} = Toska.KVStore.get(:bad)
    assert {:error, :invalid_key} = Toska.KVStore.delete(:bad)
    assert {:error, :invalid_payload} = Toska.KVStore.put(:bad, "1")
    assert {:error, :invalid_payload} = Toska.KVStore.put("a", :bad)
    assert {:error, :invalid_keys} = Toska.KVStore.mget("nope")
  end

  test "put with non-positive ttl deletes key" do
    assert :ok = Toska.KVStore.put("gone", "1", 0)
    assert {:error, :not_found} = Toska.KVStore.get("gone")
  end

  test "ttl accepts numeric strings and ignores invalid strings" do
    assert :ok = Toska.KVStore.put("ttl_str", "ok", "10")
    :timer.sleep(20)
    assert {:error, :not_found} = Toska.KVStore.get("ttl_str")

    assert :ok = Toska.KVStore.put("ttl_invalid", "ok", "bad")
    :timer.sleep(20)
    assert {:ok, "ok"} = Toska.KVStore.get("ttl_invalid")
  end

  test "list_keys honors prefix and limit" do
    assert :ok = Toska.KVStore.put("a2", "2")
    assert :ok = Toska.KVStore.put("a1", "1")
    assert :ok = Toska.KVStore.put("b1", "3")

    assert {:ok, []} = Toska.KVStore.list_keys("", 0)

    assert {:ok, keys} = Toska.KVStore.list_keys("a", 1)
    assert length(keys) == 1
    assert Enum.all?(keys, &String.starts_with?(&1, "a"))

    assert {:ok, all_a} = Toska.KVStore.list_keys("a", 10)
    assert all_a == ["a1", "a2"]
  end

  test "list_keys validates arguments" do
    assert {:error, :invalid_prefix} = Toska.KVStore.list_keys(:bad, 10)
    assert {:error, :invalid_prefix} = Toska.KVStore.list_keys("ok", -1)
  end

  test "list_keys drops expired entries" do
    assert :ok = Toska.KVStore.put("soon", "v", 5)
    :timer.sleep(10)
    assert {:ok, keys} = Toska.KVStore.list_keys("", 10)
    refute "soon" in keys
  end

  describe "list_keys_cursor/3" do
    test "returns keys with next_cursor when more results exist" do
      for i <- 1..25 do
        key = "page:#{String.pad_leading(to_string(i), 2, "0")}"
        :ok = Toska.KVStore.put(key, to_string(i))
      end

      {:ok, page1} = Toska.KVStore.list_keys_cursor("page:", 10, nil)
      assert length(page1.keys) == 10
      assert page1.next_cursor != nil
    end

    test "paginates through all results correctly" do
      for i <- 1..25 do
        key = "cursor:#{String.pad_leading(to_string(i), 2, "0")}"
        :ok = Toska.KVStore.put(key, to_string(i))
      end

      {:ok, page1} = Toska.KVStore.list_keys_cursor("cursor:", 10, nil)
      assert length(page1.keys) == 10
      assert page1.next_cursor != nil

      {:ok, page2} = Toska.KVStore.list_keys_cursor("cursor:", 10, page1.next_cursor)
      assert length(page2.keys) == 10
      assert page2.next_cursor != nil

      # No overlap between pages
      assert MapSet.disjoint?(MapSet.new(page1.keys), MapSet.new(page2.keys))

      {:ok, page3} = Toska.KVStore.list_keys_cursor("cursor:", 10, page2.next_cursor)
      assert length(page3.keys) == 5
      assert page3.next_cursor == nil

      # All keys collected
      all_keys = page1.keys ++ page2.keys ++ page3.keys
      assert length(all_keys) == 25
      assert length(Enum.uniq(all_keys)) == 25
    end

    test "returns nil next_cursor when no more results" do
      :ok = Toska.KVStore.put("final:1", "v")
      :ok = Toska.KVStore.put("final:2", "v")

      {:ok, result} = Toska.KVStore.list_keys_cursor("final:", 10, nil)
      assert length(result.keys) == 2
      assert result.next_cursor == nil
    end

    test "returns empty keys and nil cursor when no matches" do
      {:ok, result} = Toska.KVStore.list_keys_cursor("nonexistent:", 10, nil)
      assert result.keys == []
      assert result.next_cursor == nil
    end

    test "returns error for invalid cursor" do
      assert {:error, :invalid_cursor} = Toska.KVStore.list_keys_cursor("", 10, "bad!!!")
    end

    test "returns error for cursor with mismatched prefix" do
      :ok = Toska.KVStore.put("x:1", "v")
      cursor = Toska.Cursor.encode("x:1", "x:")

      # Using cursor with different prefix should error
      assert {:error, :invalid_cursor} = Toska.KVStore.list_keys_cursor("y:", 10, cursor)
    end

    test "handles limit of 0" do
      :ok = Toska.KVStore.put("zero:1", "v")
      {:ok, result} = Toska.KVStore.list_keys_cursor("zero:", 0, nil)
      assert result.keys == []
      assert result.next_cursor == nil
    end

    test "drops expired entries during iteration" do
      :ok = Toska.KVStore.put("exp:1", "v", 5)
      :ok = Toska.KVStore.put("exp:2", "v")
      :timer.sleep(10)

      {:ok, result} = Toska.KVStore.list_keys_cursor("exp:", 10, nil)
      assert result.keys == ["exp:2"]
    end
  end

  describe "list_range/3" do
    test "returns lexicographic range pages with stable cursors" do
      for key <- ["range:03", "range:01", "other:01", "range:02", "range:04"] do
        :ok = Toska.KVStore.put(key, key)
      end

      assert {:ok, page1} = Toska.KVStore.list_range("range:", 2)
      assert Enum.map(page1.items, & &1.key) == ["range:01", "range:02"]
      assert page1.next_cursor != nil

      assert {:ok, page2} = Toska.KVStore.list_range("range:", 2, cursor: page1.next_cursor)
      assert Enum.map(page2.items, & &1.key) == ["range:03", "range:04"]
      assert page2.next_cursor == nil
    end

    test "honors an inclusive start lower bound" do
      for i <- 1..5 do
        key = "start:#{i}"
        :ok = Toska.KVStore.put(key, to_string(i))
      end

      assert {:ok, result} = Toska.KVStore.list_range("start:", 10, start: "start:3")
      assert Enum.map(result.items, & &1.key) == ["start:3", "start:4", "start:5"]
    end

    test "can include values and metadata in range items" do
      assert {:ok, lease} = Toska.KVStore.create_lease(5_000, id: "range-lease")
      assert :ok = Toska.KVStore.put("rich:1", "one", nil, lease_id: lease.id)

      assert {:ok, result} =
               Toska.KVStore.list_range("rich:", 10,
                 include_values: true,
                 include_metadata: true
               )

      assert [
               %{
                 key: "rich:1",
                 value: "one",
                 metadata: metadata
               }
             ] = result.items

      assert metadata.version == 1
      assert metadata.lease_id == lease.id
      assert metadata.expires_at == lease.expires_at
    end

    test "drops deleted and expired keys from the ordered index scan" do
      assert :ok = Toska.KVStore.put("idx:delete", "gone")
      assert :ok = Toska.KVStore.delete("idx:delete")
      assert :ok = Toska.KVStore.put("idx:expire", "gone", 5)
      assert :ok = Toska.KVStore.put("idx:keep", "ok")

      :timer.sleep(10)

      assert {:ok, result} = Toska.KVStore.list_range("idx:", 10)
      assert Enum.map(result.items, & &1.key) == ["idx:keep"]
    end

    test "rebuilds the ordered index from AOF on restart" do
      assert :ok = Toska.KVStore.put("reindex:b", "2")
      assert :ok = Toska.KVStore.put("reindex:a", "1")

      stop_store()
      start_store()

      assert {:ok, result} = Toska.KVStore.list_range("reindex:", 10)
      assert Enum.map(result.items, & &1.key) == ["reindex:a", "reindex:b"]
    end

    test "validates range options" do
      assert {:error, :invalid_args} = Toska.KVStore.list_range("", 10, start: 123)
      assert {:error, :invalid_args} = Toska.KVStore.list_range("", 10, include_values: "yes")
      assert {:error, :invalid_cursor} = Toska.KVStore.list_range("", 10, cursor: "bad!!!")
    end
  end

  test "replace_snapshot validates payload" do
    assert {:error, :invalid_snapshot} = Toska.KVStore.replace_snapshot("nope")

    payload = %{
      "version" => 1,
      "created_at" => System.system_time(:millisecond),
      "checksum" => "bad",
      "data" => %{"ghost" => %{"value" => "1", "expires_at" => nil}}
    }

    assert {:error, :invalid_checksum} = Toska.KVStore.replace_snapshot(payload)
  end

  test "replace_snapshot applies data when checksum matches" do
    data = %{"keep" => %{"value" => "1", "expires_at" => nil}}
    checksum = snapshot_checksum(data)

    payload = %{
      "version" => 1,
      "created_at" => System.system_time(:millisecond),
      "checksum" => checksum,
      "data" => data
    }

    assert :ok = Toska.KVStore.replace_snapshot(payload)
    assert {:ok, "1"} = Toska.KVStore.get("keep")
  end

  test "snapshot and stats return metadata" do
    assert :ok = Toska.KVStore.put("snap", "1")

    assert :ok = Toska.KVStore.snapshot()
    {:ok, snapshot_path} = Toska.KVStore.snapshot_path()
    assert File.exists?(snapshot_path)

    {:ok, aof_path} = Toska.KVStore.aof_path()
    assert File.exists?(aof_path)

    assert {:ok, stats} = Toska.KVStore.stats()
    assert stats.keys >= 1
    assert stats.aof_path == aof_path
    assert stats.snapshot_path == snapshot_path

    assert {:ok, info} = Toska.KVStore.replication_info()
    assert info.snapshot_path == snapshot_path
    assert info.aof_path == aof_path
  end

  test "apply_replication rejects invalid input" do
    assert {:error, :invalid_replication_record} = Toska.KVStore.apply_replication("bad")
  end

  test "compaction rewrites snapshot and truncates AOF" do
    assert :ok = Toska.KVStore.put("alpha", "1")

    {:ok, aof_path} = Toska.KVStore.aof_path()
    {:ok, snapshot_path} = Toska.KVStore.snapshot_path()

    assert File.stat!(aof_path).size > 0
    assert :ok = Toska.KVStore.compact()
    assert File.stat!(snapshot_path).size > 0
    assert File.stat!(aof_path).size == 0
  end

  defp checksum(record) do
    record
    |> Enum.map(fn {key, value} -> [to_string(key), value] end)
    |> Enum.sort_by(&List.first/1)
    |> Jason.encode!()
    |> sha256()
  end

  defp snapshot_checksum(data) do
    data
    |> canonical_json()
    |> sha256()
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

  defp sha256(data) do
    :crypto.hash(:sha256, data)
    |> Base.encode16(case: :lower)
  end

  defp stop_store do
    case GenServer.whereis(Toska.KVStore) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  defp start_store do
    child_spec = %{
      id: {:kv_store, System.unique_integer([:positive])},
      start: {Toska.KVStore, :start_link, [[]]},
      restart: :temporary
    }

    start_supervised!(child_spec)
  end
end
