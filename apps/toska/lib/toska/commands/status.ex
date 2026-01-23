defmodule Toska.Commands.Status do
  @moduledoc """
  Status command for Toska.

  Displays current status of the Toska server and system information.
  """

  @behaviour Toska.Commands.Command

  alias Toska.Commands.Command
  alias Toska.ServerControl

  @impl true
  def execute(args) do
    {options, remaining_args, invalid} =
      Command.parse_options(
        args,
        [
          verbose: :boolean,
          json: :boolean,
          help: :boolean
        ],
        v: :verbose,
        j: :json,
        h: :help
      )

    cond do
      options[:help] ->
        show_help()

      invalid != [] ->
        Command.show_error("Invalid options: #{inspect(invalid)}")
        show_help()
        {:error, :invalid_options}

      true ->
        show_status(options, remaining_args)
    end
  end

  @impl true
  def show_help do
    IO.puts("""
    Show the status of the Toska server

    Usage:
      toska status [options]

    Options:
      -v, --verbose   Show detailed status information
      -j, --json      Output status in JSON format
      -h, --help      Show this help

    Examples:
      toska status
      toska status --verbose
      toska status --json
    """)

    :ok
  end

  defp show_status(options, _remaining_args) do
    verbose = options[:verbose] || false
    json = options[:json] || false

    status_info = get_status_info(verbose)

    if json do
      IO.puts(Jason.encode!(status_info, pretty: true))
    else
      display_status_text(status_info, verbose)
    end

    :ok
  end

  defp get_status_info(verbose) do
    server_status = ServerControl.status()

    base_info = %{
      server_status: server_status.status,
      uptime: server_status.uptime,
      version: Toska.version(),
      timestamp: DateTime.utc_now()
    }

    if verbose do
      Map.merge(base_info, %{
        system_info: %{
          node: Node.self(),
          otp_release: :erlang.system_info(:otp_release),
          elixir_version: System.version(),
          memory_usage: :erlang.memory() |> Enum.into(%{}),
          process_count: :erlang.system_info(:process_count)
        },
        server_details: server_status
      })
    else
      base_info
    end
  end

  defp display_status_text(status_info, verbose) do
    IO.puts("Toska Server Status")
    IO.puts("==================")
    IO.puts("")

    status_color =
      case status_info.server_status do
        :running -> "✓ Running"
        :stopped -> "✗ Stopped"
        :error -> "⚠ Error"
        _ -> "? Unknown"
      end

    IO.puts("Status: #{status_color}")
    IO.puts("Version: #{status_info.version}")

    if status_info.uptime do
      IO.puts("Uptime: #{format_uptime(status_info.uptime)}")
    end

    IO.puts("Checked at: #{DateTime.to_string(status_info.timestamp)}")

    if verbose and status_info[:system_info] do
      IO.puts("")
      IO.puts("System Information")
      IO.puts("------------------")
      sys = status_info.system_info
      IO.puts("Node: #{sys.node}")
      IO.puts("OTP Release: #{sys.otp_release}")
      IO.puts("Elixir Version: #{sys.elixir_version}")
      IO.puts("Process Count: #{sys.process_count}")
      IO.puts("Memory Usage: #{format_memory(sys.memory_usage.total)} total")
    end
  end

  defp format_uptime(uptime_ms) when is_integer(uptime_ms) do
    seconds = div(uptime_ms, 1000)
    minutes = div(seconds, 60)
    hours = div(minutes, 60)
    days = div(hours, 24)

    cond do
      days > 0 -> "#{days}d #{rem(hours, 24)}h #{rem(minutes, 60)}m"
      hours > 0 -> "#{hours}h #{rem(minutes, 60)}m #{rem(seconds, 60)}s"
      minutes > 0 -> "#{minutes}m #{rem(seconds, 60)}s"
      true -> "#{seconds}s"
    end
  end

  defp format_uptime(_), do: "Unknown"

  defp format_memory(bytes) when is_integer(bytes) do
    cond do
      bytes >= 1024 * 1024 * 1024 -> "#{Float.round(bytes / (1024 * 1024 * 1024), 2)} GB"
      bytes >= 1024 * 1024 -> "#{Float.round(bytes / (1024 * 1024), 2)} MB"
      bytes >= 1024 -> "#{Float.round(bytes / 1024, 2)} KB"
      true -> "#{bytes} B"
    end
  end
end
