defmodule Raf.PeerSupervisor do
  @moduledoc false

  use Supervisor

  alias Raf.Opts

  @type peer() :: atom() | {atom(), atom()}

  def start_link({me, %Opts{}=opts}) do
    case me do
      {name, _node} ->
        Supervisor.start_link(__MODULE__, {name, me, opts}, name: sup_name(name))
      name ->
        Supervisor.start_link(__MODULE__, {name, me, opts}, name: sup_name(name))
    end
  end

  @spec sup_name(Raft.peer()) :: atom()
  def sup_name({name, _node}) do
    sup_name(name)
  end
  def sup_name(name) do
    :"#{name}_sup"
  end

  def init({name, me, opts}) do
    children = [
      {Raf.Log, [name, opts]},
      {Raf.Server, [name, me, opts]},
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
