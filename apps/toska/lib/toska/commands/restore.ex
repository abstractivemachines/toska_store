defmodule Toska.Commands.Restore do
  @moduledoc """
  Restore command for Toska.

  Restores the KV store from a backup created by `toska backup`.
  """

  @behaviour Toska.Commands.Command

  alias Toska.Commands.Command
  alias Toska.KVStore

  @impl true
  def execute(args) do
    {options, remaining_args, invalid} =
      Command.parse_options(
        args,
        [help: :boolean, verify: :boolean, confirm: :boolean],
        [h: :help, v: :verify, y: :confirm]
      )

    cond do
      options[:help] -> show_help()
      invalid != [] -> handle_invalid(invalid)
      length(remaining_args) != 1 -> handle_missing_path()
      options[:verify] -> verify_backup(List.first(remaining_args))
      true -> restore_backup(List.first(remaining_args), options[:confirm] || false)
    end
  end

  @impl true
  def show_help do
    IO.puts("""
    Restore the Toska KV store from a backup

    Usage:
      toska restore <path> [options]

    Arguments:
      <path>    Backup directory to restore from

    Options:
      -v, --verify   Only verify backup integrity, don't restore
      -y, --confirm  Skip confirmation prompt
      -h, --help     Show this help

    Examples:
      toska restore /var/backups/toska/
      toska restore ./backup --verify
      toska restore ./backup --confirm

    WARNING: This will replace all current data!
    """)

    :ok
  end

  defp verify_backup(path) do
    with {:ok, meta} <- read_metadata(path),
         :ok <- verify_files(path, meta) do
      Command.show_success("Backup verified successfully")
      IO.puts("  Created: #{meta["created_at"]}")
      IO.puts("  Version: #{meta["version"]}")
      IO.puts("  Snapshot size: #{meta["snapshot_size"]} bytes")
      IO.puts("  AOF size: #{meta["aof_size"]} bytes")
      :ok
    else
      {:error, :no_metadata} ->
        Command.show_error("Backup metadata not found. Is this a valid backup directory?")
        {:error, :no_metadata}

      {:error, :checksum_mismatch} ->
        Command.show_error("Backup verification failed: checksum mismatch")
        {:error, :checksum_mismatch}

      {:error, reason} ->
        Command.show_error("Backup verification failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp restore_backup(path, confirmed) do
    unless confirmed do
      IO.write("This will replace ALL current data. Continue? (y/N): ")

      case IO.read(:stdio, :line) do
        {:error, _} ->
          Command.show_info("Restore cancelled")
          return_error(:cancelled)

        :eof ->
          Command.show_info("Restore cancelled")
          return_error(:cancelled)

        input ->
          input_lower = input |> String.trim() |> String.downcase()

          unless input_lower in ["y", "yes"] do
            Command.show_info("Restore cancelled")
            return_error(:cancelled)
          end
      end
    end

    with {:ok, meta} <- read_metadata(path),
         :ok <- verify_files(path, meta),
         :ok <- do_restore(path) do
      Command.show_success("Restore completed successfully")
      :ok
    else
      {:error, :cancelled} ->
        {:error, :cancelled}

      {:error, :no_metadata} ->
        Command.show_error("Backup metadata not found")
        {:error, :no_metadata}

      {:error, :checksum_mismatch} ->
        Command.show_error("Backup verification failed: checksum mismatch")
        {:error, :checksum_mismatch}

      {:error, reason} ->
        Command.show_error("Restore failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp return_error(reason), do: {:error, reason}

  defp read_metadata(path) do
    meta_path = Path.join(path, "backup_meta.json")

    with {:ok, content} <- File.read(meta_path),
         {:ok, meta} <- Jason.decode(content) do
      {:ok, meta}
    else
      {:error, :enoent} -> {:error, :no_metadata}
      {:error, reason} -> {:error, {:metadata_invalid, reason}}
    end
  end

  defp verify_files(path, meta) do
    snapshot_path = Path.join(path, "toska_snapshot.json")

    expected_checksum = meta["snapshot_checksum"]
    actual_checksum = file_checksum(snapshot_path)

    cond do
      expected_checksum == nil ->
        # No checksum stored, skip verification
        :ok

      actual_checksum == nil ->
        # File doesn't exist
        {:error, :snapshot_not_found}

      actual_checksum != expected_checksum ->
        {:error, :checksum_mismatch}

      true ->
        :ok
    end
  end

  defp do_restore(backup_path) do
    # Get current data paths from config
    data_dir = get_data_dir()

    snapshot_src = Path.join(backup_path, "toska_snapshot.json")
    aof_src = Path.join(backup_path, "toska.aof")
    snapshot_dest = Path.join(data_dir, "toska_snapshot.json")
    aof_dest = Path.join(data_dir, "toska.aof")

    # Stop store if running
    case GenServer.whereis(KVStore) do
      nil -> :ok
      _pid -> KVStore.stop()
    end

    # Ensure data directory exists
    File.mkdir_p!(data_dir)

    # Copy files
    if File.exists?(snapshot_src) do
      File.cp!(snapshot_src, snapshot_dest)
    end

    if File.exists?(aof_src) do
      File.cp!(aof_src, aof_dest)
    end

    Command.show_info("Backup files copied to #{data_dir}")
    Command.show_info("Restart the server to load restored data")
    :ok
  end

  defp get_data_dir do
    case System.get_env("TOSKA_DATA_DIR") do
      nil ->
        case GenServer.whereis(Toska.ConfigManager) do
          nil ->
            Path.join([Toska.ConfigManager.config_dir(), "data"])

          _pid ->
            case Toska.ConfigManager.list() do
              {:ok, config} -> config["data_dir"] || default_data_dir()
              _ -> default_data_dir()
            end
        end

      "" ->
        default_data_dir()

      dir ->
        dir
    end
  end

  defp default_data_dir do
    Path.join([Toska.ConfigManager.config_dir(), "data"])
  end

  defp file_checksum(path) do
    case File.read(path) do
      {:ok, content} -> :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)
      {:error, _} -> nil
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
