defmodule Raf.Requester do
  require Logger

  alias Raf.{
    Msg
  }

  @spec send(atom(), %Msg.AppendEntriesReq{} | %Msg.RequestVoteReq{}) :: :ok | term()
  def send(to, %Msg.AppendEntriesReq{from: from}=msg) do
    send(to, from, msg)
  end
  def send(to, %Msg.RequestVoteReq{from: from}=msg) do
    send(to, from, msg)
  end

  def send(to, from, msg) do
    spawn(fn ->
      case GenStateMachine.call(to, msg) do
        %Msg.AppendEntriesResp{}=resp ->
          GenStateMachine.cast(from, resp)
        %Msg.RequestVoteResp{}=resp ->
          GenStateMachine.cast(from, resp)
        error ->
          Logger.error fn ->
            "Error: #{inspect error} sending #{inspect msg} to #{to} from #{from}"
          end
      end
    end)
  end
end
