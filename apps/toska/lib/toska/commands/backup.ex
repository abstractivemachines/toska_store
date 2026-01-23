defmodule Toska.Commands.Backup do
  @moduledoc """
  Backup command for Toska.

  Creates a consistent backup of the KV store including snapshot and AOF.
  """

  @behaviour Toska.Commands.Command

  alias Toska.Commands.Command
  alias Toska.KVStore

  @impl true
  def execute(args) do
    {options, remaining_args, invalid} =
      Command.parse_options(
        args,
        [help: :boolean, force: :boolean],
        [h: :help, f: :force]
      )

    cond do
      options[:help] -> show_help()
      invalid != [] -> handle_invalid(invalid)
      length(remaining_args) != 1 -> handle_missing_path()
      true -> create_backup(List.first(remaining_args), options[:force] || false)
    end
  end

  @impl true
  def show_help do
    IO.puts("""
    Create a backup of the Toska KV store

    Usage:
      toska backup <path> [options]

    Arguments:
      <path>    Destination directory for backup files

    Options:
      -f, --force    Overwrite existing backup files
      -h, --help     Show this help

    Examples:
      toska backup /var/backups/toska/
      toska backup ./backup --force

    The backup includes:
      - toska_snapshot.json  Current data snapshot
      - toska.aof            Append-only log since snapshot
      - backup_meta.json     Backup metadata and checksums
    """)

    :ok
  end

  defp create_backup(path, force) do
    with :ok <- ensure_store_running(),
         :ok <- ensure_directory(path, force),
         {:ok, snapshot_path} <- KVStore.snapshot_path(),
         {:ok, aof_path} <- KVStore.aof_path(),
         :ok <- KVStore.snapshot(),
         :ok <- copy_files(snapshot_path, aof_path, path),
         :ok <- write_metadata(path, snapshot_path, aof_path) do
      Command.show_success("Backup created at #{path}")
      :ok
    else
      {:error, :store_not_running} ->
        Command.show_error("KV store is not running. Start the server first.")
        {:error, :store_not_running}

      {:error, :backup_exists} ->
        Command.show_error("Backup already exists. Use --force to overwrite.")
        {:error, :backup_exists}

      {:error, {:mkdir_failed, reason}} ->
        Command.show_error("Failed to create backup directory: #{inspect(reason)}")
        {:error, {:mkdir_failed, reason}}

      {:error, {:copy_failed, src, reason}} ->
        Command.show_error("Failed to copy #{src}: #{inspect(reason)}")
        {:error, {:copy_failed, src, reason}}

      {:error, reason} ->
        Command.show_error("Backup failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp ensure_store_running do
    case GenServer.whereis(KVStore) do
      nil -> {:error, :store_not_running}
      _pid -> :ok
    end
  end

  defp ensure_directory(path, force) do
    case File.mkdir_p(path) do
      :ok ->
        existing = Path.join(path, "backup_meta.json")

        if File.exists?(existing) and not force do
          {:error, :backup_exists}
        else
          :ok
        end

      {:error, reason} ->
        {:error, {:mkdir_failed, reason}}
    end
  end

  defp copy_files(snapshot_path, aof_path, dest_path) do
    snapshot_dest = Path.join(dest_path, Path.basename(snapshot_path))
    aof_dest = Path.join(dest_path, Path.basename(aof_path))

    with :ok <- safe_copy(snapshot_path, snapshot_dest),
         :ok <- safe_copy(aof_path, aof_dest) do
      :ok
    end
  end

  defp safe_copy(src, dest) do
    case File.cp(src, dest) do
      :ok -> :ok
      # File doesn't exist yet, OK
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:copy_failed, src, reason}}
    end
  end

  defp write_metadata(path, snapshot_path, aof_path) do
    meta = %{
      "created_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => Toska.version(),
      "snapshot_checksum" => file_checksum(snapshot_path),
      "aof_checksum" => file_checksum(aof_path),
      "snapshot_size" => file_size(snapshot_path),
      "aof_size" => file_size(aof_path)
    }

    meta_path = Path.join(path, "backup_meta.json")
    File.write(meta_path, Jason.encode!(meta, pretty: true))
  end

  defp file_checksum(path) do
    case File.read(path) do
      {:ok, content} -> :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)
      {:error, _} -> nil
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  defp handle_invalid(invalid) do
    Command.show_error("Invalid options: #{inspect(invalid)}")
    {:error, :invalid_options}
  end

  defp handle_missing_path do
    Command.show_error("Backup path required")
    show_help()
    {:error, :missing_path}
  end
end
