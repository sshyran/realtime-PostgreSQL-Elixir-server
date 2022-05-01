# This file draws from https://github.com/phoenixframework/phoenix/blob/9941711736c8464b27b40914a4d954ed2b4f5958/lib/phoenix/channel/server.ex
# License: https://github.com/phoenixframework/phoenix/blob/518a4640a70aa4d1370a64c2280d598e5b928168/LICENSE.md

defmodule Realtime.MessageDispatcher do
  @doc """
  Hook invoked by Phoenix.PubSub dispatch.
  """
  def dispatch(
        [_ | _] = topic_subscriptions,
        _from,
        msg
      ) do
    Enum.reduce(topic_subscriptions, %{}, fn
      {_pid, {:fastlane, fastlane_pid, serializer, _event_intercepts}}, cache ->
        broadcast_message(cache, fastlane_pid, msg, serializer)

      _, cache ->
        cache
    end)

    :ok
  end

  def dispatch(_, _, _), do: :ok

  defp broadcast_message(cache, fastlane_pid, {id, msg}, serializer) do
    {cache, payload_size} =
      case cache do
        %{^serializer => {encoded_msg, payload_size}} ->
          send(fastlane_pid, encoded_msg)
          {cache, payload_size}

        %{} ->
          encoded_msg = serializer.fastlane!(msg)
          send(fastlane_pid, encoded_msg)
          payload_size = encoded_msg |> :erlang.term_to_binary() |> :erlang.byte_size()
          {Map.put(cache, serializer, {encoded_msg, payload_size}), payload_size}
      end

    Realtime.Log.Manager.add_log(%{
      "message" => "Message sent to fastlane",
      "metadata" => %{
        "fastlane_pid" => inspect(fastlane_pid),
        "message_id" => id,
        "payload_json_bytes" => payload_size,
        "timestamp" => inspect(:os.system_time(:microsecond))
      }
    })

    cache
  end
end
