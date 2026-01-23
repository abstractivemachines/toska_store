defmodule Toska.Cursor do
  @moduledoc """
  Cursor encoding/decoding for paginated key listing.

  Cursors encode the last seen key and prefix to enable stable pagination.
  Format: Base64URL(JSON({k: last_key, p: prefix}))
  """

  @doc """
  Encode a cursor from the last key and prefix.

  ## Examples

      iex> Toska.Cursor.encode("user:100", "user:")
      "eyJrIjoidXNlcjoxMDAiLCJwIjoidXNlcjoifQ"

  """
  @spec encode(String.t(), String.t()) :: String.t()
  def encode(last_key, prefix) when is_binary(last_key) and is_binary(prefix) do
    %{"k" => last_key, "p" => prefix}
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  @doc """
  Decode a cursor, returning {:ok, {last_key, prefix}} or {:error, :invalid_cursor}.

  ## Examples

      iex> Toska.Cursor.decode("eyJrIjoidXNlcjoxMDAiLCJwIjoidXNlcjoifQ")
      {:ok, {"user:100", "user:"}}

      iex> Toska.Cursor.decode("invalid!!!")
      {:error, :invalid_cursor}

  """
  @spec decode(String.t()) :: {:ok, {String.t(), String.t()}} | {:error, :invalid_cursor}
  def decode(cursor) when is_binary(cursor) and cursor != "" do
    with {:ok, json} <- Base.url_decode64(cursor, padding: false),
         {:ok, %{"k" => key, "p" => prefix}} when is_binary(key) and is_binary(prefix) <-
           Jason.decode(json) do
      {:ok, {key, prefix}}
    else
      _ -> {:error, :invalid_cursor}
    end
  end

  def decode(_), do: {:error, :invalid_cursor}
end
