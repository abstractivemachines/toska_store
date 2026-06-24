defmodule Toska.RouterKVTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias Toska.TestHelpers

  @opts Toska.Router.init([])
  @replication_token "replication-test-token"

  setup do
    original_data_dir = System.get_env("TOSKA_DATA_DIR")
    original_auth_token = System.get_env("TOSKA_AUTH_TOKEN")
    original_replication_auth_token = System.get_env("TOSKA_REPLICATION_AUTH_TOKEN")
    original_rate_limit_per = System.get_env("TOSKA_RATE_LIMIT_PER_SEC")
    original_rate_limit_burst = System.get_env("TOSKA_RATE_LIMIT_BURST")
    original_replica_url = System.get_env("TOSKA_REPLICA_URL")
    tmp_dir = Path.join([System.tmp_dir!(), "toska_router_#{System.unique_integer([:positive])}"])

    File.mkdir_p!(tmp_dir)
    System.put_env("TOSKA_DATA_DIR", tmp_dir)
    System.delete_env("TOSKA_AUTH_TOKEN")
    System.put_env("TOSKA_REPLICATION_AUTH_TOKEN", @replication_token)
    System.delete_env("TOSKA_RATE_LIMIT_PER_SEC")
    System.delete_env("TOSKA_RATE_LIMIT_BURST")
    System.delete_env("TOSKA_REPLICA_URL")

    stop_store()
    start_store()
    Toska.RateLimiter.init()
    Toska.RateLimiter.reset()

    on_exit(fn ->
      stop_store()

      case original_data_dir do
        nil -> System.delete_env("TOSKA_DATA_DIR")
        value -> System.put_env("TOSKA_DATA_DIR", value)
      end

      restore_env("TOSKA_AUTH_TOKEN", original_auth_token)
      restore_env("TOSKA_REPLICATION_AUTH_TOKEN", original_replication_auth_token)
      restore_env("TOSKA_RATE_LIMIT_PER_SEC", original_rate_limit_per)
      restore_env("TOSKA_RATE_LIMIT_BURST", original_rate_limit_burst)
      restore_env("TOSKA_REPLICA_URL", original_replica_url)

      File.rm_rf(tmp_dir)
    end)

    :ok
  end

  test "PUT/GET/DELETE flow" do
    put_conn =
      conn("PUT", "/kv/alpha", Jason.encode!(%{value: "1"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert put_conn.status == 200

    get_conn =
      conn("GET", "/kv/alpha")
      |> Toska.Router.call(@opts)

    assert get_conn.status == 200
    get_body = Jason.decode!(get_conn.resp_body)
    assert get_body["value"] == "1"
    assert get_body["metadata"]["version"] == 1
    assert get_resp_header(get_conn, "etag") == ["\"1\""]

    delete_conn =
      conn("DELETE", "/kv/alpha")
      |> Toska.Router.call(@opts)

    assert delete_conn.status == 200

    missing_conn =
      conn("GET", "/kv/alpha")
      |> Toska.Router.call(@opts)

    assert missing_conn.status == 404
  end

  test "PUT and DELETE support conditional writes with metadata and etags" do
    create_conn =
      conn("PUT", "/kv/cas", Jason.encode!(%{value: "1", if_absent: true}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert create_conn.status == 200
    create_body = Jason.decode!(create_conn.resp_body)
    assert create_body["metadata"]["version"] == 1
    assert get_resp_header(create_conn, "etag") == ["\"1\""]

    duplicate_conn =
      conn("PUT", "/kv/cas", Jason.encode!(%{value: "again", if_absent: true}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert duplicate_conn.status == 412

    update_conn =
      conn("PUT", "/kv/cas", Jason.encode!(%{value: "2"}))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("if-match", "\"1\"")
      |> Toska.Router.call(@opts)

    assert update_conn.status == 200
    update_body = Jason.decode!(update_conn.resp_body)
    assert update_body["value"] == "2"
    assert update_body["metadata"]["version"] == 2
    assert get_resp_header(update_conn, "etag") == ["\"2\""]

    stale_delete_conn =
      conn("DELETE", "/kv/cas?if_version=1")
      |> Toska.Router.call(@opts)

    assert stale_delete_conn.status == 412

    delete_conn =
      conn("DELETE", "/kv/cas")
      |> put_req_header("if-match", "\"2\"")
      |> Toska.Router.call(@opts)

    assert delete_conn.status == 200
  end

  test "PUT rejects malformed conditional write fields" do
    conn =
      conn("PUT", "/kv/bad-condition", Jason.encode!(%{value: "1", if_version: "nope"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
    assert Jason.decode!(conn.resp_body)["error"] == "Invalid conditions"
  end

  test "txn applies success branch when compares pass" do
    :ok = Toska.KVStore.put("txn-http:guard", "old")

    body =
      Jason.encode!(%{
        compare: [%{key: "txn-http:guard", version: 1}],
        success: [
          %{op: "put", key: "txn-http:guard", value: "new"},
          %{op: "get", key: "txn-http:guard"}
        ],
        failure: [%{op: "put", key: "txn-http:fallback", value: "no"}]
      })

    conn =
      conn("POST", "/kv/txn", body)
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    response = Jason.decode!(conn.resp_body)
    assert response["succeeded"] == true
    assert Enum.map(response["responses"], & &1["op"]) == ["put", "get"]
    assert get_in(response, ["responses", Access.at(0), "metadata", "version"]) == 2
    assert {:ok, "new"} = Toska.KVStore.get("txn-http:guard")
    assert {:error, :not_found} = Toska.KVStore.get("txn-http:fallback")
  end

  test "txn applies failure branch when compares fail" do
    body =
      Jason.encode!(%{
        compare: [%{key: "txn-http:missing", exists: true}],
        success: [%{op: "put", key: "txn-http:success", value: "yes"}],
        failure: [%{op: "put", key: "txn-http:fallback", value: "used"}]
      })

    conn =
      conn("POST", "/kv/txn", body)
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    response = Jason.decode!(conn.resp_body)
    assert response["succeeded"] == false
    assert Enum.map(response["responses"], & &1["op"]) == ["put"]
    assert {:ok, "used"} = Toska.KVStore.get("txn-http:fallback")
    assert {:error, :not_found} = Toska.KVStore.get("txn-http:success")
  end

  test "txn rejects invalid payloads" do
    conn =
      conn("POST", "/kv/txn", Jason.encode!(%{compare: [%{key: "bad"}]}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
    assert Jason.decode!(conn.resp_body)["error"] == "Invalid transaction"
  end

  test "watch streams replay events as server-sent events" do
    :ok = Toska.KVStore.put("sse:a", "1")
    :ok = Toska.KVStore.put("other:a", "skip")
    :ok = Toska.KVStore.delete("sse:a")

    conn =
      conn("GET", "/kv/watch?prefix=sse:&since_revision=0&once=true")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == ["text/event-stream; charset=utf-8"]
    assert get_resp_header(conn, "x-toska-revision") == ["3"]
    assert conn.resp_body =~ ": connected"
    assert conn.resp_body =~ "event: put"
    assert conn.resp_body =~ "event: delete"

    events = sse_events(conn.resp_body)
    assert Enum.map(events, & &1["op"]) == ["put", "delete"]
    assert Enum.map(events, & &1["key"]) == ["sse:a", "sse:a"]
    assert Enum.map(events, & &1["revision"]) == [1, 3]
  end

  test "watch streams live events until timeout" do
    task =
      Task.async(fn ->
        conn("GET", "/kv/watch?prefix=live-http:&timeout_ms=100")
        |> Toska.Router.call(@opts)
      end)

    wait_until(fn ->
      case Toska.KVStore.stats() do
        {:ok, stats} -> stats.watchers == 1
        _ -> false
      end
    end)

    :ok = Toska.KVStore.put("live-http:key", "1")

    conn = Task.await(task, 1000)
    assert conn.status == 200

    [event] = sse_events(conn.resp_body)
    assert event["op"] == "put"
    assert event["key"] == "live-http:key"
    assert event["value"] == "1"
  end

  test "watch rejects invalid revisions" do
    conn =
      conn("GET", "/kv/watch?since_revision=bad&once=true")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
    assert Jason.decode!(conn.resp_body)["error"] == "Invalid watch parameters"
  end

  test "leases can attach keys and be renewed over HTTP" do
    create_conn =
      conn("POST", "/leases", Jason.encode!(%{id: "http-lease", ttl_ms: 1_000}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert create_conn.status == 201
    lease = Jason.decode!(create_conn.resp_body)["lease"]
    assert lease["id"] == "http-lease"

    put_conn =
      conn("PUT", "/kv/http-leased", Jason.encode!(%{value: "v", lease_id: "http-lease"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert put_conn.status == 200
    put_body = Jason.decode!(put_conn.resp_body)
    assert put_body["metadata"]["lease_id"] == "http-lease"
    assert put_body["metadata"]["expires_at"] == lease["expires_at"]

    :timer.sleep(2)

    keepalive_conn =
      conn("POST", "/leases/http-lease/keepalive")
      |> Toska.Router.call(@opts)

    assert keepalive_conn.status == 200
    renewed = Jason.decode!(keepalive_conn.resp_body)["lease"]
    assert renewed["expires_at"] > lease["expires_at"]

    delete_conn =
      conn("DELETE", "/leases/http-lease")
      |> Toska.Router.call(@opts)

    assert delete_conn.status == 200

    missing_conn =
      conn("GET", "/kv/http-leased")
      |> Toska.Router.call(@opts)

    assert missing_conn.status == 404
  end

  test "lock acquire and release enforce lease ownership over HTTP" do
    for id <- ["http-lock-owner", "http-lock-contender"] do
      conn =
        conn("POST", "/leases", Jason.encode!(%{id: id, ttl_ms: 5_000}))
        |> put_req_header("content-type", "application/json")
        |> Toska.Router.call(@opts)

      assert conn.status == 201
    end

    acquire_conn =
      conn(
        "POST",
        "/locks/http-lock/acquire",
        Jason.encode!(%{lease_id: "http-lock-owner", holder: "worker"})
      )
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert acquire_conn.status == 200

    assert get_in(Jason.decode!(acquire_conn.resp_body), ["lock", "lease_id"]) ==
             "http-lock-owner"

    conflict_conn =
      conn("POST", "/locks/http-lock/acquire", Jason.encode!(%{lease_id: "http-lock-contender"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conflict_conn.status == 409

    mismatch_conn =
      conn("POST", "/locks/http-lock/release", Jason.encode!(%{lease_id: "http-lock-contender"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert mismatch_conn.status == 409

    release_conn =
      conn("POST", "/locks/http-lock/release", Jason.encode!(%{lease_id: "http-lock-owner"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert release_conn.status == 200
  end

  test "PUT with a missing lease returns 404" do
    conn =
      conn("PUT", "/kv/missing-lease", Jason.encode!(%{value: "v", lease_id: "missing"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 404
    assert Jason.decode!(conn.resp_body)["error"] == "Lease not found"
  end

  test "root endpoint returns html" do
    conn =
      conn("GET", "/")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    assert conn.resp_body =~ "Toska Server"
    assert get_resp_header(conn, "content-type") != []
  end

  test "root endpoint shows running status details" do
    port = TestHelpers.free_port()
    assert {:ok, _pid} = Toska.Server.start(host: "127.0.0.1", port: port, env: "test")

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 Toska.Server.status().status == :running
               end,
               1500
             )

    conn =
      conn("GET", "/")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    assert conn.resp_body =~ "Server Status: RUNNING"
    assert conn.resp_body =~ "Configuration:"

    :ok = Toska.Server.stop()
  end

  test "mget returns values map" do
    :ok = Toska.KVStore.put("a", "1")
    :ok = Toska.KVStore.put("b", "2")

    conn =
      conn("POST", "/kv/mget", Jason.encode!(%{keys: ["a", "b", "c"]}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    values = Jason.decode!(conn.resp_body)["values"]
    assert values["a"] == "1"
    assert values["b"] == "2"
    assert is_nil(values["c"])
  end

  test "stats returns metrics" do
    :ok = Toska.KVStore.put("stat", "ok")

    conn =
      conn("GET", "/stats")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    stats = Jason.decode!(conn.resp_body)
    assert stats["keys"] >= 1
    assert stats["aof_path"]
  end

  test "stats returns error when store is stopped" do
    stop_store()

    conn =
      conn("GET", "/stats")
      |> Toska.Router.call(@opts)

    assert conn.status == 503
  end

  test "put rejects non-string values" do
    conn =
      conn("PUT", "/kv/bad", Jason.encode!(%{value: 123}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
    assert Jason.decode!(conn.resp_body)["error"] == "Value must be a string"
  end

  test "mget rejects non-list keys" do
    conn =
      conn("POST", "/kv/mget", Jason.encode!(%{keys: "nope"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
  end

  test "list keys honors prefix and limit" do
    :ok = Toska.KVStore.put("a1", "1")
    :ok = Toska.KVStore.put("a2", "2")
    :ok = Toska.KVStore.put("b1", "3")

    conn =
      conn("GET", "/kv/keys?prefix=a&limit=1")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert length(body["keys"]) == 1
    assert Enum.all?(body["keys"], &String.starts_with?(&1, "a"))
  end

  test "range endpoint supports prefix start limit and cursor pagination" do
    for key <- ["scan:03", "scan:01", "skip:01", "scan:02", "scan:04"] do
      :ok = Toska.KVStore.put(key, key)
    end

    conn1 =
      conn("GET", "/kv?prefix=scan:&start=scan:02&limit=2")
      |> Toska.Router.call(@opts)

    assert conn1.status == 200
    page1 = Jason.decode!(conn1.resp_body)
    assert Enum.map(page1["items"], & &1["key"]) == ["scan:02", "scan:03"]
    assert page1["next_cursor"] != nil

    conn2 =
      conn("GET", "/kv?prefix=scan:&limit=2&cursor=#{page1["next_cursor"]}")
      |> Toska.Router.call(@opts)

    assert conn2.status == 200
    page2 = Jason.decode!(conn2.resp_body)
    assert Enum.map(page2["items"], & &1["key"]) == ["scan:04"]
    assert page2["next_cursor"] == nil
  end

  test "range endpoint can include values and metadata" do
    {:ok, lease} = Toska.KVStore.create_lease(5_000, id: "http-range-lease")
    :ok = Toska.KVStore.put("scan-rich:1", "value", nil, lease_id: lease.id)

    conn =
      conn("GET", "/kv?prefix=scan-rich:&include_values=true&include_metadata=true")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)

    assert [
             %{
               "key" => "scan-rich:1",
               "value" => "value",
               "metadata" => metadata
             }
           ] = body["items"]

    assert metadata["version"] == 1
    assert metadata["lease_id"] == lease.id
  end

  test "range endpoint rejects invalid parameters" do
    bad_bool_conn =
      conn("GET", "/kv?include_values=yes")
      |> Toska.Router.call(@opts)

    assert bad_bool_conn.status == 400
    assert Jason.decode!(bad_bool_conn.resp_body)["error"] == "Invalid range parameters"

    bad_cursor_conn =
      conn("GET", "/kv?cursor=invalid!!!")
      |> Toska.Router.call(@opts)

    assert bad_cursor_conn.status == 400
    assert Jason.decode!(bad_cursor_conn.resp_body)["error"] == "Invalid cursor"
  end

  test "list keys with cursor pagination" do
    for i <- 1..15 do
      key = "cur:#{String.pad_leading(to_string(i), 2, "0")}"
      :ok = Toska.KVStore.put(key, to_string(i))
    end

    # First page
    conn1 =
      conn("GET", "/kv/keys?prefix=cur:&limit=5")
      |> Toska.Router.call(@opts)

    assert conn1.status == 200
    page1 = Jason.decode!(conn1.resp_body)
    assert length(page1["keys"]) == 5
    assert page1["next_cursor"] != nil

    # Second page using cursor
    conn2 =
      conn("GET", "/kv/keys?prefix=cur:&limit=5&cursor=#{page1["next_cursor"]}")
      |> Toska.Router.call(@opts)

    assert conn2.status == 200
    page2 = Jason.decode!(conn2.resp_body)
    assert length(page2["keys"]) == 5
    assert page2["next_cursor"] != nil

    # No overlap between pages
    assert MapSet.disjoint?(MapSet.new(page1["keys"]), MapSet.new(page2["keys"]))

    # Third page (final)
    conn3 =
      conn("GET", "/kv/keys?prefix=cur:&limit=5&cursor=#{page2["next_cursor"]}")
      |> Toska.Router.call(@opts)

    assert conn3.status == 200
    page3 = Jason.decode!(conn3.resp_body)
    assert length(page3["keys"]) == 5
    assert page3["next_cursor"] == nil
  end

  test "list keys returns 400 for invalid cursor" do
    conn =
      conn("GET", "/kv/keys?cursor=invalid!!!")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
    assert Jason.decode!(conn.resp_body)["error"] == "Invalid cursor"
  end

  test "list keys returns 400 for cursor with mismatched prefix" do
    :ok = Toska.KVStore.put("mismatch:1", "v")
    cursor = Toska.Cursor.encode("mismatch:1", "mismatch:")

    conn =
      conn("GET", "/kv/keys?prefix=other:&cursor=#{cursor}")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
  end

  test "list keys rejects invalid limits" do
    invalid_conn =
      conn("GET", "/kv/keys?limit=abc")
      |> Toska.Router.call(@opts)

    assert invalid_conn.status == 400
    assert Jason.decode!(invalid_conn.resp_body)["error"] == "Invalid limit"

    negative_conn =
      conn("GET", "/kv/keys?limit=-1")
      |> Toska.Router.call(@opts)

    assert negative_conn.status == 400
    assert Jason.decode!(negative_conn.resp_body)["error"] == "Invalid limit"
  end

  test "list keys caps excessive limits" do
    conn =
      conn("GET", "/kv/keys?limit=1001")
      |> Toska.Router.call(@opts)

    assert conn.status == 400
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "Limit too large"
    assert body["max"] == 1000
  end

  test "replication aof rejects invalid offsets" do
    bad_conn =
      conn("GET", "/replication/aof?since=bad")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert bad_conn.status == 400

    negative_conn =
      conn("GET", "/replication/aof?since=-1")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert negative_conn.status == 400
  end

  test "replication aof returns 204 when offset exceeds size" do
    :ok = Toska.KVStore.put("offset", "ok")
    {:ok, aof_path} = Toska.KVStore.aof_path()
    size = File.stat!(aof_path).size

    conn =
      conn("GET", "/replication/aof?since=#{size + 1}&max_bytes=1024")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert conn.status == 204
    assert get_resp_header(conn, "x-toska-aof-size") != []
  end

  test "replication aof defaults to offset 0 when since is missing" do
    :ok = Toska.KVStore.put("default_offset", "ok")

    conn =
      conn("GET", "/replication/aof?max_bytes=1024")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert conn.status in [200, 204]
    assert get_resp_header(conn, "x-toska-aof-size") != []
  end

  test "replication aof returns error when store is stopped" do
    stop_store()

    conn =
      conn("GET", "/replication/aof?since=0")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert conn.status == 503
    assert Jason.decode!(conn.resp_body)["error"] == "AOF unavailable"
  end

  test "health returns unhealthy when server is stopped" do
    conn =
      conn("GET", "/health")
      |> Toska.Router.call(@opts)

    assert conn.status == 503
    assert Jason.decode!(conn.resp_body)["status"] == "unhealthy"
  end

  test "status returns JSON" do
    conn =
      conn("GET", "/status")
      |> Toska.Router.call(@opts)

    assert conn.status == 200
    assert Jason.decode!(conn.resp_body)["status"]
  end

  test "auth token enforces KV access" do
    System.put_env("TOSKA_AUTH_TOKEN", "secret")

    unauthorized_conn =
      conn("GET", "/kv/auth")
      |> Toska.Router.call(@opts)

    assert unauthorized_conn.status == 401

    authorized_conn =
      conn("GET", "/kv/auth")
      |> put_req_header("authorization", "Bearer secret")
      |> Toska.Router.call(@opts)

    assert authorized_conn.status in [200, 404]
  end

  test "auth token accepts x-toska-token header" do
    System.put_env("TOSKA_AUTH_TOKEN", "secret")

    conn =
      conn("GET", "/kv/auth-alt")
      |> put_req_header("x-toska-token", "secret")
      |> Toska.Router.call(@opts)

    assert conn.status in [200, 404]
  end

  test "rate limiter blocks after burst is exceeded" do
    System.put_env("TOSKA_RATE_LIMIT_PER_SEC", "1")
    System.put_env("TOSKA_RATE_LIMIT_BURST", "1")
    Toska.RateLimiter.reset()

    first_conn =
      conn("GET", "/kv/limit")
      |> Toska.Router.call(@opts)

    assert first_conn.status in [200, 404]

    second_conn =
      conn("GET", "/kv/limit")
      |> Toska.Router.call(@opts)

    assert second_conn.status == 429
  end

  test "follower mode blocks KV writes" do
    System.put_env("TOSKA_REPLICA_URL", "http://leader:4000")

    write_conn =
      conn("PUT", "/kv/readonly", Jason.encode!(%{value: "x"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert write_conn.status == 403

    txn_conn =
      conn(
        "POST",
        "/kv/txn",
        Jason.encode!(%{success: [%{op: "put", key: "readonly", value: "x"}]})
      )
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert txn_conn.status == 403

    lease_conn =
      conn("POST", "/leases", Jason.encode!(%{id: "readonly-lease", ttl_ms: 1_000}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert lease_conn.status == 403

    lock_conn =
      conn("POST", "/locks/readonly/acquire", Jason.encode!(%{lease_id: "readonly-lease"}))
      |> put_req_header("content-type", "application/json")
      |> Toska.Router.call(@opts)

    assert lock_conn.status == 403

    read_conn =
      conn("GET", "/kv/readonly")
      |> Toska.Router.call(@opts)

    assert read_conn.status in [200, 404]
  end

  test "replication endpoints return data" do
    :ok = Toska.KVStore.put("replica", "ok")

    info_conn =
      conn("GET", "/replication/info")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert info_conn.status == 200
    info = Jason.decode!(info_conn.resp_body)
    assert info["aof_path"]

    status_conn =
      conn("GET", "/replication/status")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert status_conn.status in [200, 404]

    snapshot_conn =
      conn("GET", "/replication/snapshot")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert snapshot_conn.status == 200
    snapshot = Jason.decode!(snapshot_conn.resp_body)
    assert snapshot["data"]

    aof_conn =
      conn("GET", "/replication/aof?since=0&max_bytes=1024")
      |> put_replication_auth()
      |> Toska.Router.call(@opts)

    assert aof_conn.status in [200, 204]

    if aof_conn.status == 200 do
      assert aof_conn.resp_body =~ "\"op\""
    end
  end

  test "request body too large returns 413" do
    # Set a very small body size limit
    original_max_body = System.get_env("TOSKA_MAX_BODY_SIZE")
    System.put_env("TOSKA_MAX_BODY_SIZE", "100")

    # Create a payload larger than 100 bytes
    large_value = String.duplicate("x", 200)
    body = Jason.encode!(%{value: large_value})

    conn =
      conn("PUT", "/kv/toolarge", body)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("content-length", Integer.to_string(byte_size(body)))
      |> Toska.Router.call(@opts)

    assert conn.status == 413
    assert Jason.decode!(conn.resp_body)["error"] == "Request body too large"

    # Restore original
    restore_env("TOSKA_MAX_BODY_SIZE", original_max_body)
  end

  test "request with acceptable body size succeeds" do
    body = Jason.encode!(%{value: "small"})

    conn =
      conn("PUT", "/kv/smallbody", body)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("content-length", Integer.to_string(byte_size(body)))
      |> Toska.Router.call(@opts)

    # PUT returns 200 for both new and updated keys
    assert conn.status == 200
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)

  defp put_replication_auth(conn) do
    put_req_header(conn, "authorization", "Bearer #{@replication_token}")
  end

  defp sse_events(body) do
    ~r/^data: (.+)$/m
    |> Regex.scan(body)
    |> Enum.map(fn [_line, json] -> Jason.decode!(json) end)
  end

  defp wait_until(fun, attempts \\ 20)

  defp wait_until(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_until(_fun, 0), do: flunk("condition was not met in time")

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
