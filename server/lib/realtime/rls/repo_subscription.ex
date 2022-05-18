defmodule Realtime.RLS.Repo.Subscription do
  use Ecto.Repo,
    otp_app: :realtime,
    adapter: Ecto.Adapters.Postgres
end
