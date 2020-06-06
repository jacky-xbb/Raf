defmodule Raf.Configuration do
  alias Raf.{
    Config
  }

  @type peer() :: atom() | {atom(), atom()}

  #
  # API
  #

  @spec quorum_max(peer(), %Config{} | [], map()) :: non_neg_integer()
  def quorum_max(_me, %Config{state: :blank}, _), do: 0
  def quorum_max(me, %Config{state: :stable, oldservers: old_servers}, responses) do
    quorum_max(me, old_servers, responses)
  end
  def quorum_max(me, %Config{state: :staging, oldservers: old_servers}, responses) do
    quorum_max(me, old_servers, responses)
  end
  def quorum_max(me, %Config{state: :transitional, oldservers: old, newservers: new}, responses) do
    min(quorum_max(me, old, responses), quorum_max(me, new, responses))
  end

  # Sort the values received from the peers from lowest to highest
  # Peers that haven't responded have a 0 for their value.
  # This peer (me) will always have the maximum value
  def quorum_max(_, [], _), do: 0
  def quorum_max(me, servers, responses) when rem(length(servers), 2) === 0 do
      values = sorted_values(me, servers, responses)
      Enum.at(values, div(length(values), 2) - 1)
  end
  def quorum_max(me, servers, responses) do
      values = sorted_values(me, servers, responses)
      Enum.at(values, div(length(values), 2))
  end

  @spec quorum(peer(), %Config{} | list(), map()) :: boolean()
  def quorum(_me, %Config{state: :blank}, _responses), do: false
  def quorum(me, %Config{state: :stable, oldservers: old_servers}, responses) do
    quorum(me, old_servers, responses)
  end
  def quorum(me, %Config{state: :staging, oldservers: old_servers}, responses) do
    quorum(me, old_servers, responses)
  end
  def quorum(me, %Config{state: :transitional, oldservers: old, newservers: new}, responses) do
    quorum(me, old, responses) and quorum(me, new, responses)
  end

  # responses doesn't contain a local vote which must be true if the local
  # server is a member of the consensus group. Add 1 to true_responses in
  # this case.
  def quorum(me, servers, responses) do
    true_responses = for {peer, r} <- Map.to_list(responses),
      r === true, Enum.member?(servers, peer), do: r

    case Enum.member?(servers, me) do
      true ->
        length(true_responses)+1 > length(servers)/2
      false ->
        # We are about to commit a new configuration that doesn't contain
        # the local leader. We must therefore have responses from a
        # majority of the other servers to have a quorum.
        length(true_responses) > length(servers)/2
    end
  end

  @doc """
  list of voters excluding me
  """
  @spec voters(peer(), %Config{}) :: list()
  def voters(me, config) do
    List.delete(voters(config), me)
  end

  @doc """
  list of all voters
  """
  @spec voters(%Config{}) :: list()
  def voters(%Config{state: :transitional, oldservers: old, newservers: new}) do
    Enum.uniq(old ++ new)
  end
  def voters(%Config{oldservers: old}), do: old

  @spec has_vote(peer(), %Config{}) :: boolean()
  def has_vote(_me, %Config{state: :blank}), do: false
  def has_vote(me, %Config{state: :transitional, oldservers: old, newservers: new}) do
    Enum.member?(old, me) or Enum.member?(new, me)
  end
  def has_vote(me, %Config{oldservers: old}) do
    Enum.member?(old, me)
  end

  @doc """
  All followers. In staging, some followers are not voters.
  """
  @spec followers(peer(), %Config{}) :: list()
  def followers(me, %Config{state: :transitional, oldservers: old, newservers: new}) do
    old ++ new
    |> Enum.uniq()
    |> List.delete(me)
  end
  def followers(me, %Config{state: :staging, oldservers: old, newservers: new}) do
    old ++ new
    |> Enum.uniq()
    |> List.delete(me)
  end
  def followers(me, %Config{oldservers: old}) do
    old |> List.delete(me)
  end

  @doc """
  Go right to stable mode if this is the initial configuration.
  """
  @spec reconfig(%Config{}, list()) :: %Config{}
  def reconfig(%Config{state: :blank}=config, servers) do
    %{config | state: :stable, oldservers: servers}
  end
  def reconfig(%Config{state: :stable}=config, servers) do
    %{config | state: :transitional, newservers: servers}
  end

  @spec allow_config(%Config{}, list()) :: true | {:error, atom()}
  def allow_config(%Config{state: :blank}, _new_servers), do: true
  def allow_config(%Config{state: :stable, oldservers: old_servers}, new_servers)
    when new_servers !== old_servers, do: true
  def allow_config(%Config{oldservers: old_servers}, new_servers)
    when new_servers === old_servers, do:  {:error, :not_modified}
  def allow_config(_config, _new_servers), do: {:error, :config_in_progress}

  #
  # Internal Functions
  #

  @spec sorted_values(peer(), [peer()], map()) :: [non_neg_integer()]
  defp sorted_values(me, servers, responses) do
    vals = servers
      |> Enum.map(fn s -> value(responses, s) end)
      |> Enum.sort

    case Enum.member?(servers, me) do
      true ->
        # me is always in front because it is 0 from having no response
        # Strip it off the front, and add the max to the end of the list
        [_ | t] = vals
        [Enum.max(vals) | Enum.reverse(t)] |> Enum.reverse()
      false ->
        vals
    end
  end

  @spec value(map(), peer()) :: non_neg_integer()
  defp value(responses, peer) do
    case Map.fetch(responses, peer) do
      {:ok, value} ->
        value
      :error ->
        0
    end
  end
end
