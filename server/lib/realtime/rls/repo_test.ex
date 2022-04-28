defmodule Realtime.RLS.Repo.Test do
  use Ecto.Repo,
    otp_app: :realtime,
    adapter: Ecto.Adapters.Postgres
end
