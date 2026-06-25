defmodule Toska.OpenAPISpecTest do
  use ExUnit.Case, async: true

  @http_methods ~w(delete get head options patch post put trace)

  @required_paths %{
    "/" => ~w(get),
    "/status" => ~w(get),
    "/health" => ~w(get),
    "/stats" => ~w(get),
    "/metrics" => ~w(get),
    "/admin/reload" => ~w(post),
    "/kv" => ~w(get),
    "/kv/keys" => ~w(get),
    "/kv/watch" => ~w(get),
    "/kv/txn" => ~w(post),
    "/kv/{key}" => ~w(delete get put),
    "/kv/mget" => ~w(post),
    "/leases" => ~w(post),
    "/leases/{id}/keepalive" => ~w(post),
    "/leases/{id}" => ~w(delete),
    "/locks/{name}/acquire" => ~w(post),
    "/locks/{name}/release" => ~w(post),
    "/replication/info" => ~w(get),
    "/replication/status" => ~w(get),
    "/replication/snapshot" => ~w(get),
    "/replication/aof" => ~w(get)
  }

  @required_schemas ~w(
    Error
    Entry
    EntryMetadata
    RangeResponse
    KeyListResponse
    TransactionRequest
    TransactionResponse
    WatchEvent
    Lease
    Lock
    Stats
    ReplicationInfo
  )

  @required_examples ~w(
    KeyValueExample
    TransactionRequestExample
    TransactionResponseExample
    WatchEventExample
    LeaseExample
    LockExample
    RangeExample
  )

  test "OpenAPI contract documents every current route and operation responses" do
    spec = read_spec()

    assert String.starts_with?(spec["openapi"], "3.")
    assert get_in(spec, ["info", "title"]) == "ToskaStore HTTP API"

    paths = spec["paths"]
    assert is_map(paths)

    for {path, methods} <- @required_paths do
      assert Map.has_key?(paths, path), "missing path #{path}"

      for method <- methods do
        operation = get_in(paths, [path, method])
        assert is_map(operation), "missing operation #{String.upcase(method)} #{path}"

        assert is_binary(operation["operationId"]),
               "missing operationId for #{String.upcase(method)} #{path}"

        assert is_map(operation["responses"]) and map_size(operation["responses"]) > 0,
               "missing responses for #{String.upcase(method)} #{path}"
      end
    end
  end

  test "OpenAPI contract keeps schemas and workflow examples required for clients" do
    components = read_spec()["components"]
    schemas = components["schemas"]
    examples = components["examples"]

    for schema <- @required_schemas do
      assert is_map(schemas[schema]), "missing schema #{schema}"
    end

    for example <- @required_examples do
      assert is_map(examples[example]), "missing example #{example}"
      assert Map.has_key?(examples[example], "value"), "missing value for example #{example}"
    end
  end

  test "OpenAPI operation ids are unique for generated clients" do
    operation_ids =
      read_spec()
      |> Map.fetch!("paths")
      |> Enum.flat_map(fn {_path, path_item} ->
        path_item
        |> Map.take(@http_methods)
        |> Map.values()
        |> Enum.map(& &1["operationId"])
      end)

    assert Enum.all?(operation_ids, &is_binary/1)
    assert Enum.uniq(operation_ids) == operation_ids
  end

  test "OpenAPI contract declares token auth schemes" do
    security_schemes = get_in(read_spec(), ["components", "securitySchemes"])

    assert get_in(security_schemes, ["bearerAuth", "type"]) == "http"
    assert get_in(security_schemes, ["bearerAuth", "scheme"]) == "bearer"
    assert get_in(security_schemes, ["toskaToken", "type"]) == "apiKey"
    assert get_in(security_schemes, ["toskaToken", "name"]) == "X-Toska-Token"
  end

  defp read_spec do
    __DIR__
    |> Path.join("../../..")
    |> Path.expand()
    |> Path.join("openapi.yaml")
    |> YamlElixir.read_from_file!()
  end
end
