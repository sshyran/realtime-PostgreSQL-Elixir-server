defmodule Realtime.Logflare do
  use Tesla

  adapter(Tesla.Adapter.Finch, name: Realtime.Finch, receive_timeout: 30_000)

  plug Tesla.Middleware.BaseUrl, "https://api.logflare.app"

  plug Tesla.Middleware.Headers, [
    {"x-api-key", Application.get_env(:realtime, :logflare_api_key)},
    {"content-type", "application/bert"}
  ]

  plug Tesla.Middleware.Compression, format: "gzip"
  plug Tesla.Middleware.JSON

  plug Tesla.Middleware.Retry,
    max_delay: 900,
    max_retries: 3,
    should_retry: fn
      {:ok, %{status: status}} when status >= 400 -> true
      {:ok, _} -> false
      {:error, _} -> true
    end

  @source Application.get_env(:realtime, :logflare_source_id)

  def send_logs(logs) do
    body =
      Bertex.encode(%{
        "batch" => logs,
        "source" => @source
      })

    post("/api/logs", body)
  end
end
