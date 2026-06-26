defmodule Toska.ConfigManagerTest do
  use ExUnit.Case, async: false

  setup do
    original_config_dir = System.get_env("TOSKA_CONFIG_DIR")
    tmp_dir = Path.join([System.tmp_dir!(), "toska_test_#{System.unique_integer([:positive])}"])
    was_started = app_started?(:toska)

    File.mkdir_p!(tmp_dir)
    System.put_env("TOSKA_CONFIG_DIR", tmp_dir)
    stop_app(:toska)
    stop_config_manager()
    start_config_manager()

    on_exit(fn ->
      stop_config_manager()

      case original_config_dir do
        nil -> System.delete_env("TOSKA_CONFIG_DIR")
        value -> System.put_env("TOSKA_CONFIG_DIR", value)
      end

      if was_started do
        Application.ensure_all_started(:toska)
      end

      File.rm_rf(tmp_dir)
    end)

    :ok
  end

  test "loads defaults when config is missing" do
    assert {:ok, config} = Toska.ConfigManager.list()
    assert config["port"] == 4000
    assert config["host"] == "localhost"
    assert config["env"] == "dev"
  end

  test "persists config changes to disk" do
    assert :ok = Toska.ConfigManager.set("port", 5050)
    assert {:ok, 5050} = Toska.ConfigManager.get("port")

    stop_config_manager()
    start_config_manager()
    assert {:ok, 5050} = Toska.ConfigManager.get("port")
  end

  test "reset restores defaults" do
    assert :ok = Toska.ConfigManager.set("host", "0.0.0.0")
    assert :ok = Toska.ConfigManager.reset("host")
    assert {:ok, "localhost"} = Toska.ConfigManager.get("host")
  end

  test "reset_all restores defaults" do
    assert :ok = Toska.ConfigManager.set("port", 5051)
    assert :ok = Toska.ConfigManager.reset_all()
    assert {:ok, 4000} = Toska.ConfigManager.get("port")
  end

  test "get returns not found for missing keys" do
    assert {:error, :not_found} = Toska.ConfigManager.get("missing_key")
  end

  test "list returns current config" do
    assert {:ok, config} = Toska.ConfigManager.list()
    assert is_map(config)
    assert config["host"]
  end

  test "rejects invalid config values" do
    assert {:error, _} = Toska.ConfigManager.set("port", "0")
    assert {:error, _} = Toska.ConfigManager.set("sync_mode", "bad")
    assert {:error, _} = Toska.ConfigManager.set("rate_limit_per_sec", "-1")
    assert {:error, _} = Toska.ConfigManager.set("env", "stage")
    assert {:error, _} = Toska.ConfigManager.set("log_level", "verbose")
    assert {:error, _} = Toska.ConfigManager.set("host", "")
    assert {:error, _} = Toska.ConfigManager.set("named_auth_tokens", "not-json")

    assert {:error, _} =
             Toska.ConfigManager.set(
               "named_auth_tokens",
               Jason.encode!([%{name: "bad", token: "secret", scopes: ["unknown"]}])
             )

    assert {:error, _} =
             Toska.ConfigManager.set(
               "named_auth_tokens",
               Jason.encode!([%{name: "bad name", token: "secret", scopes: ["read"]}])
             )
  end

  test "accepts unknown keys" do
    assert :ok = Toska.ConfigManager.set("custom_key", "value")
    assert {:ok, "value"} = Toska.ConfigManager.get("custom_key")
  end

  test "accepts valid configuration values" do
    tmp_dir = System.tmp_dir!()

    assert :ok = Toska.ConfigManager.set("port", 4040)
    assert :ok = Toska.ConfigManager.set("host", "127.0.0.1")
    assert :ok = Toska.ConfigManager.set("env", "prod")
    assert :ok = Toska.ConfigManager.set("log_level", "debug")
    assert :ok = Toska.ConfigManager.set("data_dir", tmp_dir)
    assert :ok = Toska.ConfigManager.set("aof_file", "toska.aof")
    assert :ok = Toska.ConfigManager.set("snapshot_file", "toska_snapshot.json")
    assert :ok = Toska.ConfigManager.set("sync_mode", "none")
    assert :ok = Toska.ConfigManager.set("sync_interval_ms", "250")
    assert :ok = Toska.ConfigManager.set("snapshot_interval_ms", 500)
    assert :ok = Toska.ConfigManager.set("ttl_check_interval_ms", 200)
    assert :ok = Toska.ConfigManager.set("compaction_interval_ms", 300)
    assert :ok = Toska.ConfigManager.set("compaction_aof_bytes", 1000)
    assert :ok = Toska.ConfigManager.set("replica_url", "http://localhost:4000")
    assert :ok = Toska.ConfigManager.set("replica_poll_interval_ms", "250")
    assert :ok = Toska.ConfigManager.set("replica_http_timeout_ms", 1500)
    assert :ok = Toska.ConfigManager.set("auth_token", "token")
    assert :ok = Toska.ConfigManager.set("read_auth_token", "read-token")
    assert :ok = Toska.ConfigManager.set("write_auth_token", "write-token")
    assert :ok = Toska.ConfigManager.set("admin_auth_token", "admin-token")
    assert :ok = Toska.ConfigManager.set("replication_auth_token", "repl-token")

    assert :ok =
             Toska.ConfigManager.set(
               "named_auth_tokens",
               Jason.encode!([
                 %{name: "ci", token: "ci-token", scopes: ["read", "write"]},
                 %{name: "ops", token: "ops-token", scopes: ["admin"]}
               ])
             )

    assert {:ok, named_tokens} = Toska.ConfigManager.get("named_auth_tokens")
    assert Enum.map(named_tokens, & &1["name"]) == ["ci", "ops"]
    assert Enum.flat_map(named_tokens, & &1["scopes"]) == ["read", "write", "admin"]

    assert :ok = Toska.ConfigManager.set("rate_limit_per_sec", "0")
    assert :ok = Toska.ConfigManager.set("rate_limit_burst", 2)
  end

  test "parses numeric strings for unknown keys" do
    assert :ok = Toska.ConfigManager.set("custom_numeric", "42")
    assert {:ok, 42} = Toska.ConfigManager.get("custom_numeric")
  end

  test "supports atom keys" do
    assert :ok = Toska.ConfigManager.set(:port, 4041)
    assert {:ok, 4041} = Toska.ConfigManager.get(:port)
  end

  test "config_dir uses home when env is blank" do
    original = System.get_env("TOSKA_CONFIG_DIR")
    System.put_env("TOSKA_CONFIG_DIR", "")

    on_exit(fn ->
      case original do
        nil -> System.delete_env("TOSKA_CONFIG_DIR")
        value -> System.put_env("TOSKA_CONFIG_DIR", value)
      end
    end)

    expected = Path.join([System.user_home(), ".toska"])
    assert Toska.ConfigManager.config_dir() == expected
  end

  test "invalid config file falls back to defaults" do
    stop_config_manager()

    config_path = Toska.ConfigManager.config_file_path()
    File.write!(config_path, "{")

    start_config_manager()

    assert {:ok, config} = Toska.ConfigManager.list()
    assert config["port"] == 4000
    assert config["host"] == "localhost"
  end

  defp app_started?(app) do
    Enum.any?(Application.started_applications(), fn {name, _, _} -> name == app end)
  end

  defp stop_app(app) do
    if app_started?(app) do
      Application.stop(app)
    end

    :ok
  end

  defp stop_config_manager do
    case GenServer.whereis(Toska.ConfigManager) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  defp start_config_manager do
    child_spec = %{
      id: {:config_manager, System.unique_integer([:positive])},
      start: {Toska.ConfigManager, :start_link, [[]]},
      restart: :temporary
    }

    start_supervised!(child_spec)
  end
end
