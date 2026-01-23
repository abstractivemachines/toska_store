defmodule Toska.CursorTest do
  use ExUnit.Case, async: true

  alias Toska.Cursor

  describe "encode/2" do
    test "encodes key and prefix to base64 url-safe string" do
      cursor = Cursor.encode("user:100", "user:")
      assert is_binary(cursor)
      # Should be valid base64 url-safe
      assert {:ok, _} = Base.url_decode64(cursor, padding: false)
    end

    test "produces different cursors for different keys" do
      cursor1 = Cursor.encode("key:1", "key:")
      cursor2 = Cursor.encode("key:2", "key:")
      assert cursor1 != cursor2
    end

    test "produces different cursors for different prefixes" do
      cursor1 = Cursor.encode("key:1", "key:")
      cursor2 = Cursor.encode("key:1", "other:")
      assert cursor1 != cursor2
    end
  end

  describe "decode/1" do
    test "decodes a valid cursor" do
      cursor = Cursor.encode("user:100", "user:")
      assert {:ok, {"user:100", "user:"}} = Cursor.decode(cursor)
    end

    test "roundtrips correctly" do
      test_cases = [
        {"simple", ""},
        {"key:with:colons", "key:"},
        {"unicode_\u{1F600}", "unicode_"},
        {"", ""},
        {"key", "key"}
      ]

      for {key, prefix} <- test_cases do
        cursor = Cursor.encode(key, prefix)
        assert {:ok, {^key, ^prefix}} = Cursor.decode(cursor)
      end
    end

    test "returns error for invalid base64" do
      assert {:error, :invalid_cursor} = Cursor.decode("not-valid-base64!!!")
    end

    test "returns error for valid base64 but invalid json" do
      bad = Base.url_encode64("not json at all", padding: false)
      assert {:error, :invalid_cursor} = Cursor.decode(bad)
    end

    test "returns error for valid json but missing keys" do
      bad = Base.url_encode64(Jason.encode!(%{"wrong" => "keys"}), padding: false)
      assert {:error, :invalid_cursor} = Cursor.decode(bad)
    end

    test "returns error for nil" do
      assert {:error, :invalid_cursor} = Cursor.decode(nil)
    end

    test "returns error for empty string" do
      assert {:error, :invalid_cursor} = Cursor.decode("")
    end

    test "returns error for non-string key in cursor" do
      bad = Base.url_encode64(Jason.encode!(%{"k" => 123, "p" => "prefix"}), padding: false)
      assert {:error, :invalid_cursor} = Cursor.decode(bad)
    end

    test "returns error for non-string prefix in cursor" do
      bad = Base.url_encode64(Jason.encode!(%{"k" => "key", "p" => 123}), padding: false)
      assert {:error, :invalid_cursor} = Cursor.decode(bad)
    end
  end
end
