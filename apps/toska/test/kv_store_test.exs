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

  test "ttl expires keys" do
    assert :ok = Toska.KVStore.put("temp", "value", 10)
    :timer.sleep(20)
    assert {:error, :not_found} = Toska.KVStore.get("temp")
  end

  test "aof replay restores values on restart" do
    assert :ok = Toska.KVStore.put("persist", "yes")
    stop_store()
    start_store()
    assert {:ok, "yes"} = Toska.KVStore.get("persist")
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
    assert :ok = Toska.KVStore.put("a1", "1")
    assert :ok = Toska.KVStore.put("a2", "2")
    assert :ok = Toska.KVStore.put("b1", "3")

    assert {:ok, []} = Toska.KVStore.list_keys("", 0)

    assert {:ok, keys} = Toska.KVStore.list_keys("a", 1)
    assert length(keys) == 1
    assert Enum.all?(keys, &String.starts_with?(&1, "a"))

    assert {:ok, all_a} = Toska.KVStore.list_keys("a", 10)
    assert Enum.sort(all_a) == ["a1", "a2"]
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
