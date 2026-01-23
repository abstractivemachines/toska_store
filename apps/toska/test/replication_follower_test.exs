defmodule Toska.TestLeaderState do
  use Agent

  def start_link do
    Agent.start_link(fn -> %{snapshot: %{"data" => %{}}, aof: ""} end, name: __MODULE__)
  end

  def set_snapshot(payload) do
    Agent.update(__MODULE__, &Map.put(&1, :snapshot, payload))
  end

  def append_aof(record) do
    line = Jason.encode!(record) <> "\n"

    Agent.update(__MODULE__, fn state ->
      %{state | aof: state.aof <> line}
    end)
  end

  def snapshot do
    Agent.get(__MODULE__, & &1.snapshot)
  end

  def aof_since(offset) do
    Agent.get(__MODULE__, fn %{aof: aof} ->
      size = byte_size(aof)

      if offset >= size do
        {"", size}
      else
        {binary_part(aof, offset, size - offset), size}
      end
    end)
  end
end

defmodule Toska.TestLeaderPlug do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    payload = Toska.TestLeaderState.snapshot()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(payload))
  end

  get "/replication/aof" do
    conn = fetch_query_params(conn)
    offset = parse_offset(conn.params["since"])
    {body, size} = Toska.TestLeaderState.aof_since(offset)

    conn =
      conn
      |> put_resp_content_type("application/octet-stream")
      |> put_resp_header("x-toska-aof-size", Integer.to_string(size))

    status = if offset >= size, do: 204, else: 200
    send_resp(conn, status, body)
  end

  match _ do
    send_resp(conn, 404, "not_found")
  end

  defp parse_offset(nil), do: 0
  defp parse_offset(offset) when is_integer(offset), do: max(offset, 0)

  defp parse_offset(offset) when is_binary(offset) do
    case Integer.parse(offset) do
      {value, ""} -> max(value, 0)
      _ -> 0
    end
  end

  defp parse_offset(_), do: 0
end

defmodule Toska.TestLeaderChunkPlug do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    payload = Toska.TestLeaderState.snapshot()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(payload))
  end

  get "/replication/aof" do
    conn = fetch_query_params(conn)
    offset = parse_offset(conn.params["since"])
    max_bytes = parse_max_bytes(conn.params["max_bytes"])
    {body, size} = Toska.TestLeaderState.aof_since(offset)

    chunk =
      if byte_size(body) > max_bytes do
        binary_part(body, 0, max_bytes)
      else
        body
      end

    conn =
      conn
      |> put_resp_content_type("application/octet-stream")
      |> put_resp_header("x-toska-aof-size", Integer.to_string(size))

    status = if offset >= size, do: 204, else: 200
    send_resp(conn, status, chunk)
  end

  match _ do
    send_resp(conn, 404, "not_found")
  end

  defp parse_offset(nil), do: 0
  defp parse_offset(offset) when is_integer(offset), do: max(offset, 0)

  defp parse_offset(offset) when is_binary(offset) do
    case Integer.parse(offset) do
      {value, ""} -> max(value, 0)
      _ -> 0
    end
  end

  defp parse_offset(_), do: 0

  defp parse_max_bytes(nil), do: 65_536

  defp parse_max_bytes(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> 65_536
    end
  end

  defp parse_max_bytes(_), do: 65_536
end

defmodule Toska.TestLeaderErrorPlug do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    send_resp(conn, 200, "{")
  end

  get "/replication/aof" do
    send_resp(conn, 204, "")
  end
end

defmodule Toska.TestLeaderAofErrorPlug do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    payload = %{"data" => %{}}

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(payload))
  end

  get "/replication/aof" do
    conn
    |> put_resp_content_type("application/octet-stream")
    |> send_resp(500, "boom")
  end
end

defmodule Toska.TestLeaderSnapshotErrorPlug do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    send_resp(conn, 500, "snapshot_fail")
  end

  get "/replication/aof" do
    send_resp(conn, 204, "")
  end
end

defmodule Toska.TestLeaderAofNoHeaderPlug do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    payload = %{"data" => %{}}

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(payload))
  end

  get "/replication/aof" do
    body =
      [
        "not-json",
        Jason.encode!(%{"op" => "set", "key" => "aof", "value" => "1"})
      ]
      |> Enum.join("\n")
      |> Kernel.<>("\n")

    conn
    |> put_resp_content_type("application/octet-stream")
    |> send_resp(200, body)
  end
end

defmodule Toska.TestLeaderCorruptedAofPlug do
  @moduledoc """
  Mock leader that returns a mix of valid JSON, incomplete JSON, and non-JSON lines.
  Used to test follower resilience to corrupted AOF data.
  """
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/replication/snapshot" do
    payload = %{"data" => %{}}

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(payload))
  end

  get "/replication/aof" do
    # Mix of valid and invalid AOF lines
    body = """
    {"op":"set","key":"valid1","value":"ok","v":1}
    {corrupted line without closing brace
    {"op":"set","key":"valid2","value":"ok","v":1}
    not json at all
    {"op":"set","key":"valid3","value":"ok","v":1}
    """

    conn
    |> put_resp_content_type("application/octet-stream")
    |> put_resp_header("x-toska-aof-size", Integer.to_string(byte_size(body)))
    |> send_resp(200, body)
  end

  match _ do
    send_resp(conn, 404, "not_found")
  end
end

defmodule Toska.ReplicationFollowerTest do
  use ExUnit.Case, async: false

  alias Toska.KVStore
  alias Toska.Replication.Follower
  alias Toska.TestHelpers
  alias Toska.TestLeaderPlug
  alias Toska.TestLeaderState

  setup context do
    original_data_dir = System.get_env("TOSKA_DATA_DIR")
    tmp_dir = TestHelpers.tmp_dir("toska_replica")

    File.mkdir_p!(tmp_dir)
    TestHelpers.put_env("TOSKA_DATA_DIR", tmp_dir)

    stop_store()
    start_store()

    {:ok, _} = TestLeaderState.start_link()
    port = TestHelpers.free_port()
    plug = Map.get(context, :leader_plug, TestLeaderPlug)
    {:ok, bandit_pid} = Bandit.start_link(plug: plug, port: port)

    on_exit(fn ->
      stop_follower()
      stop_store()
      stop_leader_state()
      stop_bandit(bandit_pid)
      TestHelpers.restore_env("TOSKA_DATA_DIR", original_data_dir)
      File.rm_rf(tmp_dir)
    end)

    %{leader_url: "http://localhost:#{port}"}
  end

  test "bootstraps from snapshot and tails aof", %{leader_url: leader_url} do
    payload = %{
      "data" => %{
        "snap" => %{"value" => "1", "expires_at" => nil}
      }
    }

    TestLeaderState.set_snapshot(payload)
    TestLeaderState.append_aof(%{"op" => "set", "key" => "aof", "value" => "2"})

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 match?({:ok, "1"}, KVStore.get("snap"))
               end,
               1000
             )

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 match?({:ok, "2"}, KVStore.get("aof"))
               end,
               1000
             )

    offset_path = Path.join(System.get_env("TOSKA_DATA_DIR"), "replica.offset")

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 case File.read(offset_path) do
                   {:ok, content} ->
                     case Integer.parse(String.trim(content)) do
                       {value, ""} when value > 0 -> true
                       _ -> false
                     end

                   _ ->
                     false
                 end
               end,
               1000
             )
  end

  test "status reports leader url", %{leader_url: leader_url} do
    TestLeaderState.set_snapshot(%{"data" => %{}})

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 100, http_timeout_ms: 1000)

    assert {:ok, status} = Follower.status()
    assert status.leader_url == leader_url
  end

  test "status returns error when follower not running" do
    stop_follower()
    assert {:error, :not_running} = Follower.status()
  end

  @tag leader_plug: Toska.TestLeaderErrorPlug
  test "bootstrap records errors for invalid snapshots", %{leader_url: leader_url} do
    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 case Follower.status() do
                   {:ok, status} -> not is_nil(status.last_error)
                   _ -> false
                 end
               end,
               1000
             )
  end

  @tag leader_plug: Toska.TestLeaderAofErrorPlug
  test "polling records aof errors", %{leader_url: leader_url} do
    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 case Follower.status() do
                   {:ok, status} -> match?({:aof_failed, 500, _}, status.last_error)
                   _ -> false
                 end
               end,
               1000
             )
  end

  @tag leader_plug: Toska.TestLeaderSnapshotErrorPlug
  test "load_offset keeps persisted values on bootstrap failure", %{leader_url: leader_url} do
    offset_path = Path.join(System.get_env("TOSKA_DATA_DIR"), "replica.offset")
    File.write!(offset_path, "25")

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 case Follower.status() do
                   {:ok, status} -> status.offset == 25
                   _ -> false
                 end
               end,
               1000
             )
  end

  @tag leader_plug: Toska.TestLeaderAofNoHeaderPlug
  test "applies aof entries without size headers", %{leader_url: leader_url} do
    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 match?({:ok, "1"}, KVStore.get("aof"))
               end,
               1000
             )

    assert {:ok, status} = Follower.status()
    assert status.offset > 0
  end

  @tag leader_plug: Toska.TestLeaderChunkPlug
  test "applies aof entries split across chunks", %{leader_url: leader_url} do
    TestLeaderState.set_snapshot(%{"data" => %{}})

    large_value = String.duplicate("x", 70_000)
    TestLeaderState.append_aof(%{"op" => "set", "key" => "chunked", "value" => large_value})

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 25, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 case KVStore.get("chunked") do
                   {:ok, value} -> value == large_value
                   _ -> false
                 end
               end,
               3000
             )
  end

  test "start_link fails without leader url" do
    previous = Process.flag(:trap_exit, true)

    assert {:error, :missing_leader_url} = Follower.start_link(leader_url: "")

    Process.flag(:trap_exit, previous)
  end

  @tag leader_plug: Toska.TestLeaderChunkPlug
  test "handles multiple entries with chunk splitting mid-json", %{leader_url: leader_url} do
    TestLeaderState.set_snapshot(%{"data" => %{}})

    # First entry: small, fits in first chunk
    TestLeaderState.append_aof(%{"op" => "set", "key" => "first", "value" => "1"})

    # Second entry: large, will be split across chunks (65,536 byte chunk size)
    large_value = String.duplicate("y", 64_000)
    TestLeaderState.append_aof(%{"op" => "set", "key" => "split", "value" => large_value})

    # Third entry: comes after the split
    TestLeaderState.append_aof(%{"op" => "set", "key" => "third", "value" => "3"})

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 25, http_timeout_ms: 1000)

    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 match?({:ok, "1"}, KVStore.get("first")) and
                   match?({:ok, ^large_value}, KVStore.get("split")) and
                   match?({:ok, "3"}, KVStore.get("third"))
               end,
               5000
             )
  end

  test "follower restarts from persisted offset", %{leader_url: leader_url} do
    TestLeaderState.set_snapshot(%{"data" => %{}})
    TestLeaderState.append_aof(%{"op" => "set", "key" => "before_restart", "value" => "1"})

    {:ok, pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    # Wait for first entry to be applied
    assert :ok =
             TestHelpers.wait_until(
               fn -> match?({:ok, "1"}, KVStore.get("before_restart")) end,
               2000
             )

    # Get current offset and verify it's persisted
    {:ok, status} = Follower.status()
    persisted_offset = status.safe_offset
    assert persisted_offset > 0

    # Stop follower
    GenServer.stop(pid)

    # Add more entries while follower is down
    TestLeaderState.append_aof(%{"op" => "set", "key" => "after_restart", "value" => "2"})

    # Restart follower - should resume from persisted offset
    {:ok, _pid2} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    # Both keys should be present - proves we didn't lose data
    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 match?({:ok, "1"}, KVStore.get("before_restart")) and
                   match?({:ok, "2"}, KVStore.get("after_restart"))
               end,
               2000
             )

    # Verify offset continued from where it left off
    {:ok, new_status} = Follower.status()
    assert new_status.offset >= persisted_offset
  end

  @tag leader_plug: Toska.TestLeaderCorruptedAofPlug
  test "skips corrupted aof lines and continues", %{leader_url: leader_url} do
    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    # Valid entries should be applied despite corrupted lines interleaved
    assert :ok =
             TestHelpers.wait_until(
               fn ->
                 match?({:ok, "ok"}, KVStore.get("valid1")) and
                   match?({:ok, "ok"}, KVStore.get("valid2")) and
                   match?({:ok, "ok"}, KVStore.get("valid3"))
               end,
               2000
             )
  end

  test "handles corrupted offset file gracefully", %{leader_url: leader_url} do
    TestLeaderState.set_snapshot(%{"data" => %{}})
    TestLeaderState.append_aof(%{"op" => "set", "key" => "after_corrupt", "value" => "v"})

    # Write invalid data to offset file
    offset_path = Path.join(System.get_env("TOSKA_DATA_DIR"), "replica.offset")
    File.write!(offset_path, "not-a-number\n")

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    # Should start from offset 0 when file is corrupted and apply entries
    assert :ok =
             TestHelpers.wait_until(
               fn -> match?({:ok, "v"}, KVStore.get("after_corrupt")) end,
               2000
             )

    {:ok, status} = Follower.status()
    assert status.offset > 0
  end

  test "handles negative offset in file gracefully", %{leader_url: leader_url} do
    TestLeaderState.set_snapshot(%{"data" => %{}})
    TestLeaderState.append_aof(%{"op" => "set", "key" => "neg_offset", "value" => "v"})

    # Write negative offset
    offset_path = Path.join(System.get_env("TOSKA_DATA_DIR"), "replica.offset")
    File.write!(offset_path, "-100")

    {:ok, _pid} =
      Follower.start_link(leader_url: leader_url, poll_interval_ms: 50, http_timeout_ms: 1000)

    # Should clamp to 0 and apply entries
    assert :ok =
             TestHelpers.wait_until(
               fn -> match?({:ok, "v"}, KVStore.get("neg_offset")) end,
               2000
             )
  end

  defp stop_store do
    case GenServer.whereis(KVStore) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  defp start_store do
    child_spec = %{
      id: {:kv_store, System.unique_integer([:positive])},
      start: {KVStore, :start_link, [[]]},
      restart: :temporary
    }

    start_supervised!(child_spec)
  end

  defp stop_follower do
    case GenServer.whereis(Follower) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
    end
  end

  defp stop_leader_state do
    case Process.whereis(TestLeaderState) do
      nil -> :ok
      pid -> Agent.stop(pid)
    end
  end

  defp stop_bandit(pid) do
    try do
      GenServer.stop(pid)
    catch
      :exit, _ -> :ok
    end
  end
end
