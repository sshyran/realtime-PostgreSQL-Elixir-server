defmodule RealtimeWeb.RealtimeChannel do
  use RealtimeWeb, :channel
  require Logger, warn: false

  def join("realtime:" <> _topic, _payload, socket) do
    send(self(), :after_join)
    {:ok, %{}, socket}
  end

  # @doc """
  # Disabling inward messages from the websocket.
  # """
  # def handle_in(event_type, payload, socket) do
  #   Logger.info event_type
  #   broadcast!(socket, event_type, payload)
  #   {:noreply, socket}
  # end

  def handle_info(:after_join, socket) do
    Realtime.Metrics.SocketMonitor.track_channel(socket)
    {:noreply, socket}
  end

  def handle_in("access_token", _, socket) do
    {:noreply, socket}
  end
end
