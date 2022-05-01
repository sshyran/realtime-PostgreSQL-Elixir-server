defmodule Realtime.Log.Manager do
  use GenServer

  alias Realtime.Logflare

  # minimum time in ms before a log batch is sent
  @flush_interval 1_000
  # maximum number of events before a log batch is sent
  @max_batch_size 100

  # Client

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def add_log(log) do
    GenServer.cast(__MODULE__, {:add, log})
  end

  # Server (callbacks)

  @impl true
  def init(_) do
    ref = send_flush()
    {:ok, %{batch: [], batch_size: 0, ref: ref}}
  end

  @impl true
  def handle_cast({:add, log}, %{batch: batch, batch_size: batch_size, ref: ref} = state) do
    batch = [log | batch]
    batch_size = batch_size + 1

    {batch, batch_size, ref} =
      if batch_size == @max_batch_size do
        Process.cancel_timer(ref)
        Logflare.send_logs(batch)
        {[], 0, send_flush()}
      else
        {batch, batch_size, ref}
      end

    {:noreply, %{state | batch: batch, batch_size: batch_size, ref: ref}}
  end

  @impl true
  def handle_info(:flush, %{batch: batch, ref: ref} = state) do
    Process.cancel_timer(ref)

    case batch do
      [_ | _] -> Logflare.send_logs(batch)
      _ -> :ok
    end

    {:noreply, %{state | batch: [], batch_size: 0, ref: send_flush()}}
  end

  defp send_flush, do: Process.send_after(self(), :flush, @flush_interval)
end
