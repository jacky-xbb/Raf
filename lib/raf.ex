defmodule Raf do

  alias Raf.{
    Log,
    Opts,
    Server
  }

  @type peer() :: atom() | {atom(), atom()}

  def start_node(peer, opts) do
    Raf.Server.Supervisor.start_peer(peer, opts)
  end

  def stop_node(peer) do
    Raf.Server.Supervisor.stop_peer(peer)
  end

  @doc """
   Run an operation on the backend state machine.
  Note: peer is just the local node in production.
  """
  def write(peer, command) do
    id = UUID.uuid4()
    Server.op(peer, {id, command})
  end

  def read(peer, command) do
    id = UUID.uuid4()
    Server.read_op(peer, {id, command})
  end

  def set_config(peer, new_servers) do
    id = UUID.uuid4()
    Server.set_config(peer, {id, new_servers})
  end

  @spec get_leader(peer()) :: term()
  def get_leader(peer) do
    Server.get_leader(peer)
  end

  @spec get_entry(peer(), non_neg_integer()) :: term()
  def get_entry(peer, index) do
    Log.get_entry(peer, index)
  end

  @spec get_last_entry(peer()) :: term()
  def get_last_entry(peer) do
    Log.get_last_entry(peer)
  end

  ## Test Functions

  def start_test_cluster() do
    path = make_path("test_log")
    opts = %Opts{state_machine: Raf.Backend.Echo, logdir: path}
    peers = [:peer1, :peer2, :peer3]
    peers
    |> Enum.each(fn peer -> start_node(peer, opts) end)

    peers
  end

  def start_test_node(name) do
    path = make_path("test_data")
    peer = {name, node()}
    opts = %Opts{state_machine: Raf.Backend.Ets, logdir: path}
    start_node(peer, opts)
  end

  defp make_path(name) do
    path =
      File.cwd!
      |> Path.join(name)

    File.rm_rf!(path)
    File.mkdir(path)
    path
  end
end
