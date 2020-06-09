defmodule Raf.Server do
  @moduledoc """
  The Server module provides the raft fsm
  """

  use GenStateMachine, callback_mode: :state_functions

  require Logger

  alias Raf.{
    Log,
    Config,
    Opts,
    Msg,
    Requester,
    Configuration
  }

  @client_timeout 2000
  @election_timeout_min 500
  @election_timeout_max 1000
  @heartbeat_timeout 25

defmodule ClientReq do
    defstruct [:id, :timer, :from, :index, :term, :cmd]

    @type t :: %__MODULE__{
      id: binary(),
      timer: reference(),
      from: term(),
      index: non_neg_integer(),
      term: non_neg_integer(),
      # only used during read_only commands
      cmd: term()
    }
  end

  defmodule State do
    defstruct [:leader, :voted_for, :init_config, :timer, :me, :config, :state_machine, :backend_state,
               term: 0, commit_index: 0, followers: %{}, responses: %{}, send_clock: 0, send_clock_responses: %{},
               client_reqs: [], read_reqs: []]

    @type t :: %__MODULE__{
      leader: term(),
      term: non_neg_integer(),
      voted_for: term(),
      commit_index: non_neg_integer(),
      init_config: :undefined | list() | :complete | :no_client,

      # Used for Election and Heartbeat timeouts
      timer: reference(),

      # leader state: contains new_index for each peer
      followers: map(),

      # Map keyed by peer id.
      # contains true as val when candidate
      # contains match_indexes as val when leader
      responses: map(),

      # Logical clock to allow read linearizability
      # Reset to 0 on leader election.
      send_clock: non_neg_integer(),

      # Keep track of the highest send_clock received from each peer
      # Reset on leader election
      send_clock_responses: map(),

      # Outstanding Client Write requests
      client_reqs: [%ClientReq{}],

      # Outstanding Client Read requests
      # Keyed on send_clock, val = [%ClientReq{}]
      read_reqs: map(),

      # All servers making up the ensemble
      me: String.t(),

      config: term(),

      # We allow pluggable backend state machine modules.
      state_machine: atom(),
      backend_state: term()
    }
  end

  # API

  def start_link(name, me, opts) do
    GenStateMachine.start_link(__MODULE__, [me, opts], name: name)
  end

  def op(peer, command) do
    GenStateMachine.call(peer, {:op, command})
  end

  def read_op(peer, command) do
    GenStateMachine.call(peer, {:read_op, command})
  end

  def set_config(peer, config) do
    GenStateMachine.call(peer, {:set_config, config})
  end

  def get_leader(pid) do
    GenStateMachine.call(pid, :get_leader)
  end

  @spec send(atom(), %Msg.RequestVoteResp{} | %Msg.AppendEntriesResp{}) :: :ok
  def send(to, msg) do
    # Catch badarg error thrown if name is unregistered
    try do
      GenStateMachine.cast(to, msg)
    rescue
      _e -> Logger.error("name is unregistered!")
    end
  end

  @spec send_sync(atom(), %Msg.AppendEntriesReq{} | %Msg.RequestVoteReq{}) ::
      %Msg.AppendEntriesResp{} | %Msg.RequestVoteResp{} | :timeout
  def send_sync(to, msg) do
    GenStateMachine.call(to, msg, 100)
  end

  def stop(pid) do
    GenStateMachine.call(pid, :stop)
  end

  #
  # gen_state_machine callbacks
  #

  def init([me, %Opts{state_machine: state_machine}]) do
    timer = Process.send_after(self(), :timeout, election_timeout())
    %Log.Meta{voted_for: voted_for, term: term} = Log.get_metadata(me)
    backend_state = state_machine.init(me)
    state = %State{
      term: term,
      voted_for: voted_for,
      me: me,
      responses: %{},
      followers: %{},
      timer: timer,
      state_machine: state_machine,
      backend_state: backend_state
    }
    config = Log.get_config(me)
    state =
      case config.state do
        :blank ->
          %{state | config: config}
        _ ->
          %{state | config: config, init_config: :complete}
      end
    {:ok, :follower, state}
  end

  def format_status(reason, [_pdict, state, data]) do
    case reason do
      :terminate ->
        Logger.error("terminates abnormally or logs an error!")
      _ ->
        [
          state: [{'State', "Current state: '#{inspect state}'"}],
          data: [{'Data', "Current data: '#{inspect data}'"}]
        ]
    end
  end

  def handle_event({:call, from}, :get_leader, state_name, %State{leader: leader}=state) do
    {:next_state, state_name, state, [{:reply, from, leader}]}
  end
  # def handle_event(_event_type, _event_content, _state_name, state) do
  #   {:stop, :badmsg, state}
  # end

  def handle_event(
        :info,
        {:client_read_timeout, clock, id},
        state_name,
        %State{read_reqs: reqs}=state
      ) do
    {:ok, client_requests} = Map.fetch(reqs, clock)
    {:ok, client_req} = find_client_req(id, client_requests)
    send_client_timeout_reply(client_req)
    client_requests = delete_client_req(id, client_requests)
    new_reqs = Map.put(reqs, clock, client_requests)
    state = %{state | read_reqs: new_reqs}
    {:next_state, state_name, state}
  end

  def handle_event(
        :info,
        {:client_timeout, id},
        state_name,
        %State{client_reqs: reqs}=state) do
    case find_client_req(id, reqs) do
      {:ok, client_req} ->
        send_client_timeout_reply(client_req)
        state = %{state | client_reqs: delete_client_req(Id, reqs)}
        {:next_state, state_name, state}
      :not_found ->
        {:next_state, state_name, state}
    end
  end

  def handle_info(:info, _, _, state) do
    {:stop, :badmsg, state}
  end

  def terminate(_, _, _) do
    :ok
  end

  def code_change(_old_vsn, state_name, state, _extra) do
    {:ok, state_name, state}
  end

  #
  # Follower States
  #

  # Election timeout has expired. Go to candidate state if we are a voter.
  def follower(:info, :timeout, %State{config: config, me: me}=state) do
    case Configuration.has_vote(me, config) do
      false ->
        state = reset_timer(election_timeout(), state)
        state = %{state | leader: :undefined}
        {:next_state, :follower, state}
      true ->
        state = become_candidate(state)
        {:next_state, :candidate, state}
    end
  end

  # Ignore stale messages.
  def follower(:cast, %Msg.RequestVoteReq{}, _state) do
    # {:next_state, :follower, state}
    :keep_state_and_data
  end
  def follower(:cast, %Msg.AppendEntriesResp{}, _state) do
    # {:next_state, :follower, state}
    :keep_state_and_data
  end

  # Vote for this candidate
  def follower({:call, from}, %Msg.RequestVoteReq{}=request_vote_req, state) do
    handle_request_vote(from, request_vote_req, state)
  end

  def follower(
      {:call, from},
      %Msg.AppendEntriesReq{term: term},
      %State{term: current_term, me: me}=_state
    )
    when current_term > term  do
    resp = %Msg.AppendEntriesResp{from: me, term: current_term, success: false}
    {:keep_state_and_data, [{:reply, from, resp}]}
  end

  def follower(
    {:call, from},
    %Msg.AppendEntriesReq{
      term: term,
      from: from,
      prev_log_index: prev_log_index,
      entries: entries,
      commit_index: commit_index,
      send_clock: clock}=append_entries,
    %State{me: me}=state
    ) do
    state2 = set_term(term , state)
    resp = %Msg.AppendEntriesResp{
      send_clock: clock,
      term: term,
      success: false,
      from: me}
    # Always reset the election timer here, since the leader is valid,
    # but may have conflicting data to sync
    state3 = reset_timer(election_timeout(), state2)
    case consistency_check(append_entries, state3) do
      false ->
        # {reply, resp, follower, state3}
        {:keep_state, state3, [{:reply, from, resp}]}

      true ->
        {:ok, current_index} = Log.check_and_append(me, entries, prev_log_index+1)
        config = Log.get_config(me)
        resp = %{resp | success: true, index: current_index}
        state4 = commit_entries(commit_index, state3)
        state5 = %{state4 | leader: from, config: config}
        # {reply, resp, follower, state5}
        {:keep_state, state5, [{:reply, from, resp}]}
    end
  end

  # Allow setting config in follower state only if the config is blank
  # (e.g. the log is empty). A config entry must always be the first
  # entry in every log.
  def follower(
        {:call, from},
        {:set_config, {id, new_servers}},
        %State{me: me, followers: f, config: %Config{state: :blank}=c}=state
      ) do
    case Enum.member?(new_servers, me) do
      true ->
        {followers, config} = reconfig(me, f, c, new_servers, state)
        new_state = %{state | config: config, followers: followers, init_config: [id, from]}
        # Transition to candidate state. Once we are elected leader we will
        # send the config to the other machines. We have to do it this way
        # so that the entry we log will have a valid term and can be
        # committed without a noop.  Note that all other configs must
        # be blank on the other machines.
        {:next_state, :candidate, new_state}
      false ->
          error = {:error, :not_consensus_group_member}
          # {reply, Error, follower, State}
          {:keep_state_and_data, [{:reply, from, error}]}
    end
  end

  def follower(
        {:call, from},
        {:set_config, _},
        %State{leader: :undefined, me: me, config: c}=_state
      ) do
    error = no_leader_error(me, c)
    # {reply, {error, Error}, follower, State}
    {:keep_state_and_data, [{:reply, from, {:error, error}}]}
  end

  def follower(
        {:call, from},
        {:set_config, _},
        %State{leader: leader}=_state
      ) do
    error = {:error, {:redirect, leader}}
    # {reply, Reply, follower, State}
    {:keep_state_and_data, [{:reply, from, error}]}
  end

  def follower(
        {:call, from},
        {:read_op, _},
        %State{me: me, config: config, leader: :undefined}=_state
      ) do
    error = no_leader_error(me, config)
    # {reply, {error, Error}, follower, State}
    {:keep_state_and_data, [{:reply, from, error}]}
  end

  def follower(
        {:call, from},
        {:read_op, _},
        %State{leader: leader}=_state
      ) do
    reply = {:error, {:redirect, leader}}
    # {reply, reply, follower, State};
    {:keep_state_and_data, [{:reply, from, reply}]}
  end

  def follower(
        {:call, from},
        {:op, _command},
        %State{me: me, config: config, leader: :undefined}=_state
      ) do
    error = no_leader_error(me, config)
    {:keep_state_and_data, [{:reply, from, error}]}
  end

  def follower(
        {:call, from},
        {:op, _command},
        %State{leader: leader}=_state
      ) do
    reply = {:error, {:redirect, leader}}
    {:keep_state_and_data, [{:reply, from, reply}]}
  end

  #
  # Candidate States
  #


  # This is the initial election to set the initial config. We did not
  # get a quorum for our votes, so just reply to the user here and keep trying
  # until the other nodes come up.
  def candidate(:info, :timeout, %State{term: 1, init_config: [_id, from]}=state) do
    state = reset_timer(election_timeout(), state)
    GenStateMachine.reply(from, {:error, :peers_not_responding})
    state = %{state | init_config: :no_client}
    # {:next_state, :candidate, state}
    {:keep_state, state}
  end

  # The election timeout has elapsed so start an election
  def candidate(:info, :timeout, state) do
    state = become_candidate(state)
    # {next_state, candidate, new_state}
    {:keep_state, state}
  end

  # This should only happen if two machines are configured differently during
  # initial configuration such that one configuration includes both proposed leaders
  # and the other only itself. Additionally, there is not a quorum of either
  # configuration's servers running.

  # (i.e. raf.set_config(:b, [:k, :b, :j]), raf.set_config(:d, [:i,:k,:b,:d,:o]).
  #       when only b and d are running.)
  def candidate(
        :cast,
        %Msg.RequestVoteResp{term: vote_term, success: false},
        %State{term: term, init_config: [_id, from]}=state
      )
      when vote_term > term do
    GenStateMachine.reply(from, {:error, :invalid_initial_config})
    state = %{state | init_config: :undefined, config: %Config{state: :blank}}
    new_state = step_down(vote_term, state)
    {:next_state, :follower, new_state}
  end

  # We are out of date. Go back to follower state.
  def candidate(
        :cast,
        %Msg.RequestVoteResp{term: vote_term, success: false},
        %State{term: term}=state
      )
      when vote_term > term do
    new_state = step_down(vote_term, state)
    {:next_state, :follower, new_state}
  end

  # This is a stale vote from an old request. Ignore it.
  def candidate(
        :cast,
        %Msg.RequestVoteResp{term: vote_term},
        %State{term: current_term}=_state
      )
      when vote_term < current_term do
    :keep_state_and_data
  end

  def candidate(
        :cast,
        %Msg.RequestVoteResp{success: false, from: from},
        %State{responses: responses}=state
      ) do
    responses = Map.put(responses, from, false)
    new_state = %{state | responses: responses}
    {:keep_state, new_state}
  end

  # Sweet, someone likes us! Do we have enough votes to get elected?
  def candidate(
        :cast,
        %Msg.RequestVoteResp{success: true, from: from},
        %State{responses: responses, me: me, config: config}=state
      ) do
    responses = Map.put(responses, from, true)
    case Configuration.quorum(me, config, responses) do
      true ->
        new_state = become_leader(state)
        {:next_state, :leader, new_state}
      false ->
        state = %{state | responses: responses}
        {:keep_state, state}
    end
  end

  def candidate({:call, from}, {:set_config, _}, state) do
    reply = {:error, :election_in_progress}
    {:next_state, :follower, state, [{:reply, from, reply}]}
  end

  # A peer is simultaneously trying to become the leader
  # If it has a higher term, step down and become follower.
  def candidate(
        {:call, from},
        %Msg.RequestVoteReq{term: request_term}=request_vote,
        %State{term: term}=state
      )
      when request_term > term do
    state = step_down(request_term, state)
    handle_request_vote(from, request_vote, state)
  end

  def candidate(
        {:call, from},
        %Msg.RequestVoteReq{},
        %State{term: current_term, me: me}=_state
      ) do
    reply = %Msg.RequestVoteResp{term: current_term, success: false, from: me}
    {:keep_state_and_data, [{:reply, from, reply}]}
  end

  # Another peer is asserting itself as leader, and it must be correct because
  # it was elected. We are still in initial config, which must have been a
  # misconfiguration. Clear the initial configuration and step down. Since we
  # still have an outstanding client request for inital config send an error
  # response.
  def candidate(
        {:call, _from},
        %Msg.AppendEntriesReq{term: request_term},
        %State{init_config: [_, client]}=state
      ) do
    GenStateMachine.reply(client, {:error, :invalid_initial_config})
    # Set to complete, we don't want another misconfiguration
    state = %{state | init_config: :complete, config: %Config{state: :blank}}
    state = step_down(request_term, state)
    # TODO: consider timeout
    {:next_state, :follower, state}
  end

  # Same as the above clause, but we don't need to send an error response.
  def candidate(
        {:call, _from},
        %Msg.AppendEntriesReq{term: request_term},
        %State{init_config: :no_client}=state
      ) do
    # Set to complete, we don't want another misconfiguration
    state = %{state | init_config: :complete, config: %Config{state: :blank}}
    state = step_down(request_term, state)
    # TODO: consider timeout
    {:next_state, :follower, state}
  end

  # Another peer is asserting itself as leader. If it has a current term
  # step down and become follower. Otherwise do nothing
  def candidate(
        {:call, _from},
        %Msg.AppendEntriesReq{term: request_term},
        %State{term: current_term}=state
      )
      when request_term >= current_term do
    state = step_down(request_term, state)
    # TODO: consider timeout
    {:next_state, :follower, state}
  end

  def candidate(:cast, %Msg.AppendEntriesReq{}, state) do
    {:next_state, :candidate, state}
  end

  # We are in the middle of an election.
  # Leader should always be undefined here.
  def candidate(
        {:call, from},
        {:read_op, _},
        %State{leader: :undefined}=_state
      ) do
    {:keep_state_and_data, [{:reply, from, {:error, :election_in_progress}}]}
  end
  def candidate(
        {:call, from},
        {:op, _command},
        %State{leader: :undefined}=_state
      ) do
    {:keep_state_and_data, [{:reply, from, {:error, :election_in_progress}}]}
  end


  #
  # Leader States
  #

  def leader(:info, :timeout, state) do
    state = reset_timer(@heartbeat_timeout, state)
    state = send_append_entries(state)
    {:keep_state, state}
  end

  # We are out of date. Go back to follower state.
  def leader(
        :cast,
        %Msg.AppendEntriesResp{term: term, success: false},
        %State{term: current_term}=state
      )
      when term > current_term do
    state = step_down(term, state)
    {:next_state, :follower, state}
  end

  # This is a stale reply from an old request. Ignore it.
  def leader(
        :cast,
        %Msg.AppendEntriesResp{term: term, success: true},
        %State{term: current_term}=_state
      )
      when current_term > term do
    :keep_state_and_data
  end

  # The follower is not synced yet. Try the previous entry
  def leader(
        :cast,
        %Msg.AppendEntriesResp{from: from, success: false},
        %State{followers: followers, config: c, me: me}=state
      ) do
    case Enum.member?(Configuration.followers(me, c), from) do
      true ->
        next_index = decrement_follower_index(followers, from)
        next_followers = Map.put(followers, from, next_index)
        state = %{state | followers: next_followers}
        {:keep_state, state}
      false ->
        # This is a reply from a previous configuration. Ignore it.
        :keep_state_and_data
    end
  end

  # Success!
  def leader(
        :cast,
        %Msg.AppendEntriesResp{from: from, success: true}=resp,
        %State{followers: followers, config: c, me: me}=state
      ) do
    case Enum.member?(Configuration.followers(me, c), from) do
      true ->
        state = save_resp(resp, state)
        state = maybe_commit(state)
        state = maybe_send_read_replies(state)
        case state.leader do
          :undefined ->
            # We just committed a config that doesn't include ourselves
            {:next_state, :follower, state}
          _ ->
            state = maybe_increment_follower_index(from, followers, state)
            {:keep_state, state}
        end
      false ->
        # This is a reply from a previous configuration. Ignore it.
        :keep_state_and_data
    end
  end

  # Ignore stale votes.
  def leader(:cast, %Msg.RequestVoteResp{}, _state) do
    :keep_state_and_data
  end

  # An out of date leader is sending append_entries, tell it to step down.
  def leader(
        {:call, from},
        %Msg.AppendEntriesReq{term: term},
        %State{term: current_term, me: me}=state
      )
      when term < current_term do
    resp = %Msg.AppendEntriesResp{from: me, term: current_term, success: false}
    {:keep_state, state, [{:reply, from, resp}]}
  end

  # We are out of date. Step down
  def leader(
        {:call, _from},
        %Msg.AppendEntriesReq{term: term},
        %State{term: current_term}=state
      )
      when term > current_term do
    state = step_down(term, state)
    # TODO: consider timeout
    {:next_state, :follower, state}
  end

  # We are out of date. Step down
  def leader(
        {:call, _from},
        %Msg.RequestVoteReq{term: term},
        %State{term: current_term}=state
      )
      when term > current_term do
    state = step_down(term, state)
    # TODO: consider timeout
    {:next_state, :follower, state}
  end

  # An out of date candidate is trying to steal our leadership role. Stop it.
  def leader(
        {:call, from},
        %Msg.RequestVoteReq{},
        %State{me: me, term: current_term}=_state
      ) do
    resp = %Msg.RequestVoteResp{from: me, term: current_term, success: false}
    {:keep_state_and_date, [{:reply, from, resp}]}
  end

  def leader(
        {:call, from},
        {:set_config, {id, new_servers}},
        %State{me: me, followers: f, term: term, config: c}=state
      ) do
    case Configuration.allow_config(c, new_servers) do
        true ->
          {followers, config} = reconfig(me, f, c, new_servers, state)
          entry = %Log.Entry{type: :config, term: term, cmd: config}
          state = %{state | config: config, followers: followers}
          state = append(id, from, entry, state, :leader)
          # TODO: consider timeout
          {:keep_state, state}
        error ->
          {:keep_state_and_data, [{:reply, from, error}]}
    end
  end

  # Handle client requests
  def leader({:call, from}, {:read_op, {id, command}}, state) do
    state = setup_read_request(id, from, command, state)
    # TODO: consider timeout
    {:keep_state, state}
  end

  def leader(
        {:call, from},
        {:op, {id, command}},
        %State{term: term}=state
      ) do
    entry = %Log.Entry{type: :op, term: term, cmd: command}
    state = append(id, from, entry, state, :leader)
    # TODO: consider timeout
    {:keep_state, state}
  end

  #
  # Internal Functions
  #

  defp maybe_increment_follower_index(from, followers, %State{me: me}=state) do
    last_log_index = Log.get_last_index(me)
    {:ok, index} = Map.fetch(followers, from)

    case index <= last_log_index do
      true ->
        %{state | followers: Map.put(followers, from, index+1)}
      false ->
        state
    end
  end

  defp find_eligible_read_requests(send_clock, %State{read_reqs: requests}=state) do
    eligible =
      requests
      |> Enum.take_while(fn {clock, _} -> send_clock > clock end)

    requests =
      requests
      |> Enum.drop_while(fn {clock, _} -> send_clock > clock end)

    state = %{state | read_reqs: requests}
    {:ok, eligible, state}
  end

  defp send_client_read_replies([], state) do
    state
  end
  defp send_client_read_replies(
        requests,
        %State{state_machine: state_machine, backend_state: backend_state} = state
      ) do
    backend_state = List.foldl(requests, backend_state, fn ({_clock, client_reqs}, be_state) ->
      read_and_send(client_reqs, state_machine, be_state)
    end)
    %{state | backend_state: backend_state}
  end

  defp read_and_send(client_requests, state_machine, backend_state) do
    List.foldl(client_requests, backend_state, fn (req, acc) ->
      {val, new_acc} = state_machine.read(req.cmd, acc)
      send_client_reply(req, val)
      new_acc
    end)
  end

  defp maybe_send_read_replies(
        %State{me: me,
        config: config,
        send_clock_responses: responses} = state
      ) do
    clock = Configuration.quorum_max(me, config, responses)
    {:ok, requests, state} = find_eligible_read_requests(clock, state)
    state = send_client_read_replies(requests, state)
    state
  end

  defp send_client_timeout_reply(%ClientReq{from: from}) do
    GenStateMachine.reply(from, {:error, :timeout})
  end

  defp find_client_req(id, client_requests) do
    result = Enum.filter(client_requests, fn req ->
      req.id === id
    end)
    case result do
      [request] ->
        {:ok, request}
      [] ->
        :not_found
    end
  end

  defp delete_client_req(id, client_requests) do
    Enum.filter(client_requests, fn req ->
      req.id !== id
    end)
  end

  defp setup_read_request(id, from, command, %State{send_clock: clock, me: me, term: term}=state) do
    timer = Process.send_after(me, {:client_read_timeout, clock, id}, @client_timeout)
    read_request = %ClientReq{id: id,
                              from: from,
                              term: term,
                              cmd: command,
                              timer: timer}
    state = save_read_request(read_request, state)
    send_append_entries(state)
  end

  defp save_read_request(
        read_request,
        %State{send_clock: clock, read_reqs: requests}=state
      ) do
    new_requests =
      case Map.fetch(requests, clock) do
        {:ok, read_requests} ->
          Map.put(requests, clock, [read_request | read_requests])
        :error ->
          Map.put(requests, clock, [read_request])
      end
      %{state | read_reqs: new_requests}
  end

  def safe_to_commit(index, %State{term: current_term, me: me}) do
    current_term === Log.get_term(me, index)
  end

  defp maybe_commit(
        %State{
          me: me,
          commit_index: commit_index,
          config: config,
          responses: responses}=state
        ) do
    min = Configuration.quorum_max(me, config, responses)
    case min > commit_index and safe_to_commit(min, state) do
      true ->
        new_state = commit_entries(min, state)
        case Configuration.has_vote(me, new_state.config) do
          true ->
            new_state
          false ->
            # We just committed a config that doesn't include ourself
            step_down(new_state.term, new_state)
        end
      false ->
        state
    end
  end

  defp save_resp(
        %Msg.AppendEntriesResp{from: from, index: index, send_clock: clock},
        %State{responses: responses, send_clock_responses: clock_responses}=state
      ) do
    new_responses = save_greater(from, index, responses)
    new_clock_responses = save_greater(from, clock, clock_responses)
    %{state | responses: new_responses, send_clock_responses: new_clock_responses}
  end

  defp save_greater(key, val, map) do
    current_val = Map.fetch(map, key)
    save_greater(map, key, val, current_val)
  end

  defp save_greater(map, _key, val, {:ok, current_val}) when current_val > val do
    map
  end
  defp save_greater(map, _key, current_val, {:ok, current_val}) do
    map
  end
  defp save_greater(map, key, val, {:ok, _}) do
    Map.put(map, key, val)
  end
  defp save_greater(map, key, val, :error) do
    Map.put(map, key, val)
  end

  defp decrement_follower_index(followers, from) do
    case Map.fetch(followers, from) do
      {:ok, 1} ->
        1
      {:ok, num} ->
        num - 1
    end
  end

  @spec append(%Log.Entry{}, %State{}) :: %State{}
  defp append(entry, %State{me: me}=state) do
    {:ok, _index} = Log.append(me, [entry])
    send_append_entries(state)
  end

  @spec append(binary(), term(), %Log.Entry{}, %State{}, :leader) :: %State{}
  defp append(id, from, entry, state, :leader) do
    state = append(id, from, entry, state)
    send_append_entries(state)
  end

  @spec append(binary(), term(), %Log.Entry{}, %State{}) :: %State{}
  defp append(id, from, entry, %State{me: me, term: term, client_reqs: reqs}=state) do
    {:ok, index} = Log.append(me, [entry])
    timer = Process.send_after(me, {:client_timeout, id}, @client_timeout)
    client_request = %ClientReq{
      id: id,
      from: from,
      index: index,
      term: term,
      timer: timer}
    %{state | client_reqs: [client_request | reqs]}
  end

  defp initialize_followers(%State{me: me, config: config}) do
    peers = Configuration.followers(me, config)
    next_index = Log.get_last_index(me) + 1
    peers
    |> Enum.map(fn peer -> {peer, next_index} end)
    |> Map.new()
  end

  defp become_leader(%State{me: me, term: term, config: config, init_config: init_config}=state) do
    state = %{state | leader: me,
                      responses: %{},
                      followers: initialize_followers(state),
                      send_clock: 0,
                      send_clock_responses: %{},
                      read_reqs: %{}}

    case init_config do
      :complete ->
        # Commit a noop entry to the log so we can move the commit index
        entry = %Log.Entry{type: :noop, term: term, cmd: :noop}
        append(entry, state)
      :undefined ->
        # Same as above, but we received our config from another node
        entry = %Log.Entry{type: :noop, term: term, cmd: :noop}
        state = append(entry, state)
        %{state | init_config: :complete}
      [id, from] ->
        # Initial config, append it, and set init_config=:complete
        entry = %Log.Entry{type: :config, term: term, cmd: config}
        state = append(id, from, entry, state, :leader)
        state = reset_timer(@heartbeat_timeout, state)
        %{state | init_config: :complete}
      :no_client ->
        # Same as above, but no-one to tell
        entry = %Log.Entry{type: :config, term: term, cmd: config}
        state = append(entry, state)
        state = reset_timer(@heartbeat_timeout, state)
        %{state | init_config: :complete}
    end
  end

  # We are about to transition to the follower state. Reset the necessary state.
  # TODO: send errors to any outstanding client read or write requests and cleanup
  # timers
  defp step_down(new_term, state) do
    state = reset_timer(election_timeout(), state)
    state = %{state | term: new_term, responses: %{}, leader: :undefined}
    set_metadata(:undefined, state)
  end

  defp no_leader_error(me, config) do
    case Configuration.has_vote(me, config) do
      false ->
        :not_consensus_group_member
      true ->
        :election_in_progress
    end
  end

  @spec add_followers(list(), map(), %State{}) :: map()
  defp add_followers(new_servers, followers, %State{me: me}) do
    next_index = Log.get_last_index(me) + 1
    new_followers = for s <- new_servers do
      {s, next_index}
    end
    Map.new(new_followers ++ Map.to_list(followers))
  end

  @spec remove_followers(list(), map()) :: map()
  defp remove_followers(servers, followers0) do
    List.foldl(servers, followers0, fn(s, followers) ->
      Map.delete(followers, s)
    end)
  end

  @spec reconfig(term(), map(), %Config{}, list(), %State{}) :: {map(), %Config{}}
  defp reconfig(me, old_followers, config0, new_servers, state) do
    config = Configuration.reconfig(config0, new_servers)
    new_followers = Configuration.followers(me, config)

    old_set = old_followers
    |> MapSet.to_list()
    |> Enum.map(fn {k, _} -> k end)
    |> MapSet.new()
    new_set = new_followers |> MapSet.new()

    added_servers = MapSet.to_list(MapSet.difference(new_set, old_set))
    removed_servers = MapSet.to_list(MapSet.difference(old_set, new_set))
    followers0 = add_followers(added_servers, old_followers, state)
    followers = remove_followers(removed_servers, followers0)

    {followers, config}
  end

  defp get_prev(me, index) do
    case index - 1 do
      0 ->
        {0, 0}
      prev_index ->
        {prev_index, Log.get_term(me, prev_index)}
    end
  end

  # TODO: Return a block of entries if more than one exist
  defp get_entries(me, index) do
    case Log.get_entry(me, index) do
      {:ok, :not_found} ->
        []
      {:ok, entry} ->
        [entry]
    end
  end

  defp send_entry(peer, index, %State{me: me,
                                      term: term,
                                      send_clock: clock,
                                      commit_index: c_idx}) do
    {prev_log_index, prev_log_term} = get_prev(me, index)
    entries = get_entries(me, index)
    append_entries = %Msg.AppendEntriesReq{term: term,
                                           from: me,
                                           prev_log_index: prev_log_index,
                                           prev_log_term: prev_log_term,
                                           entries: entries,
                                           commit_index: c_idx,
                                           send_clock: clock}
    Requester.send(peer, append_entries)
  end

  defp send_append_entries(%State{followers: followers, send_clock: send_clock}=state) do
    new_state = %{state | send_clock: send_clock+1}
    followers
    |> Map.to_list()
    |> Enum.each(fn {peer, index} ->
      send_entry(peer, index, new_state)
    end)
    new_state
  end

  defp find_client_req_by_index(index, client_requests) do
    result = Enum.filter(client_requests, fn req ->
      req.index === index
    end)
    case result do
      [request] ->
        {:ok, request}
      [] ->
        :not_found
    end
  end

  defp send_client_reply(%ClientReq{timer: timer, from: from}, result) do
    :ok = Process.cancel_timer(timer)
    GenStateMachine.reply(from, result)
  end

  defp delete_client_req_by_index(index, client_requests) do
    Enum.filter(client_requests, fn req ->
      req.index !== index
    end)
  end

  @spec maybe_send_client_reply(non_neg_integer(), [%ClientReq{}], %State{}, term()) :: %State{}
  defp maybe_send_client_reply(index, cli_reqs, %State{leader: leader, me: me}=s, result)
      when leader === me do
    case find_client_req_by_index(index, cli_reqs) do
      {:ok, req} ->
        send_client_reply(req, result)
        reqs = delete_client_req_by_index(index, cli_reqs)
        %{s | client_reqs: reqs}
      :not_found ->
        s
    end
  end

  defp maybe_send_client_reply(_, _, state, _) do
    state
  end

  @spec stabilize_config(%Config{}, %State{}) :: %State{}
  defp stabilize_config(%Config{state: :transitional, newservers: new}=c,
    %State{leader: leader, me: me, term: term}=s) when leader === me do
    config = %{c | state: :stable, oldservers: new, newservers: []}
    entry = %Log.Entry{type: :config, term: term, cmd: config}
    state = %{s | config: config}
    {:ok, _index} = Log.append(me, [entry])
    send_append_entries(state)
  end
  defp stabilize_config(_, state) do
    state
  end

  # Commit entries between the previous commit index and the new one.
  # Apply them to the local state machine and respond to any outstanding
  # client requests that these commits affect. Return the new state.
  # Ignore already committed entries.
  @spec commit_entries(non_neg_integer(), %State{}) :: %State{}
  defp commit_entries(new_commit_index, %State{commit_index: commit_index}=state)
    when commit_index >= new_commit_index do
    state
  end
  defp commit_entries(new_commit_index, %State{
    commit_index: commit_index,
    state_machine: state_machine,
    backend_state: backend_state,
    me: me}=state) do
   last_index = min(Log.get_last_index(me), new_commit_index)
   index_range = commit_index+1..last_index
   List.foldl(index_range, state, fn index, %State{client_reqs: cli_reqs}=state1 ->
      new_state = %{state1 | commit_index: index}
      case Log.get_entry(me, index) do
        # Noop - Ignore this request
        {:ok, %Log.Entry{type: :noop}} ->
          new_state

        # Normal Operation. Apply command to state_machine.
        {:ok, %Log.Entry{type: :op, cmd: command}} ->
          {result, new_backend_state} = state_machine.write(command, backend_state)
          new_state2 = %{new_state | backend_state: new_backend_state}
          maybe_send_client_reply(index, cli_reqs, new_state2, result)

        # We have a committed transitional state, so reply
        # successfully to the client. Then set the new stable
        # configuration.
        {:ok, %Log.Entry{type: :config, cmd: %Config{state: :transitional}=c}} ->
          s = stabilize_config(c, new_state)
          reply = {:ok, s.config}
          maybe_send_client_reply(index, cli_reqs, s, reply)

        # The configuration has already been set. Initial configuration goes
        # directly to stable state so needs to send a reply. Checking for
        # a client request is expensive, but config changes happen
        # infrequently.
        {:ok, %Log.Entry{type: :config, cmd: %Config{state: :stable}}} ->
          reply = {:ok, new_state.config}
          maybe_send_client_reply(index, cli_reqs, new_state, reply)
      end
   end)
  end

  # There is no entry at t=0, so just return true.
  defp consistency_check(%Msg.AppendEntriesReq{
    prev_log_index: 0,
    prev_log_term: 0}, _state) do
    true
  end
  defp consistency_check(%Msg.AppendEntriesReq{
    prev_log_index: index,
    prev_log_term: term},
    %State{me: me}) do
    case Log.get_entry(me, index) do
      {:ok, :not_found} ->
        false
      {:ok, %Log.Entry{term: ^term}} ->
        true
      {:ok, %Log.Entry{term: _different_term}} ->
        false
    end
  end

  defp set_term(term, %State{term: current_term}=state) when term < current_term do
    state
  end
  defp set_term(term, %State{term: current_term}=state) when term > current_term do
    set_metadata(:undefined, %{state | term: term})
  end
  defp set_term(term , %State{term: term}=state) do
    state
  end

  defp successful_vote(current_term, me) do
    {:ok, %Msg.RequestVoteResp{term: current_term, success: true, from: me}}
  end

  defp fail_vote(current_term, me) do
    {:ok, %Msg.RequestVoteResp{term: current_term, success: false, from: me}}
  end

  defp maybe_successful_vote(request_vote_req, current_term, me, state) do
    case candidate_log_up_to_date(request_vote_req, state) do
      true ->
        successful_vote(current_term, me)
      false ->
        fail_vote(current_term, me)
    end
  end

  defp candidate_log_up_to_date(%Msg.RequestVoteReq{last_log_term: candidate_term,
                                                    last_log_index: candidate_index},
                                %State{me: me}) do
    candidate_log_up_to_date(candidate_term,
                             candidate_index,
                             Log.get_last_term(me),
                             Log.get_last_index(me))
  end

  defp candidate_log_up_to_date(candidate_term, _candidate_index, log_term, _log_index)
                                when candidate_term > log_term do
    true
  end
  defp candidate_log_up_to_date(candidate_term, _candidate_index, log_term, _log_index)
                                when candidate_term < log_term do
    false
  end
  defp candidate_log_up_to_date(term, candidate_index, term, log_index)
                                when candidate_index > log_index do
    true
  end
  defp candidate_log_up_to_date(term, candidate_index, term, log_index)
                                when candidate_index < log_index do
    false
  end
  defp candidate_log_up_to_date(term, index, term, index) do
    true
  end

  defp vote(%Msg.RequestVoteReq{term: term}, %State{term: current_term, me: me})
            when term < current_term do
      fail_vote(current_term, me)
  end
  defp vote(%Msg.RequestVoteReq{from: candidate_id, term: current_term}=request_vote_req,
            %State{voted_for: candidate_id, term: current_term, me: me}=state) do
    maybe_successful_vote(request_vote_req, current_term, me, state)
  end
  defp vote(%Msg.RequestVoteReq{term: current_term}=request_vote_req,
            %State{voted_for: :undefined, term: current_term, me: me}=state) do
    maybe_successful_vote(request_vote_req, current_term, me, state)
  end
  defp vote(%Msg.RequestVoteReq{from: candidate_id, term: current_term},
                                %State{voted_for: another_id, term: current_term, me: me})
                                when another_id !== candidate_id do
    fail_vote(current_term, me)
  end

  defp handle_request_vote(
        from,
        %Msg.RequestVoteReq{from: candidate_id, term: term}=request_vote_req,
        state
      ) do
    state = set_term(term, state)
    {:ok, resp} = vote(request_vote_req, state)
    case resp.success do
      true ->
        state = set_metadata(candidate_id, state)
        state = reset_timer(election_timeout(), state)
        {:next_state, :follower, state, [{:reply, from, resp}]}
      false ->
        {:next_state, :follower, state, [{:reply, from, resp}]}
    end
  end

  # Start a process to send a syncrhonous rpc to each peer. Votes will be sent
  # back as messages when the process receives them from the peer. If
  # there is an error or a timeout no message is sent. This helps preserve
  # the asynchrnony of the consensus fsm, while maintaining the rpc
  # semantics for the request_vote message as described in the raft paper.

  defp request_votes(%State{config: config, term: term, me: me}) do
    voters = Configuration.voters(me, config)
    msg = %Msg.RequestVoteReq{
      term: term,
      from: me,
      last_log_index: Log.get_last_index(me),
      last_log_term: Log.get_last_term(me)
    }
    Enum.each(voters, fn peer ->
      Requester.send(peer, msg)
    end)
  end

  @spec become_candidate(%State{}) :: %State{}
  defp become_candidate(%State{term: current_term, me: me}=state) do
    state = reset_timer(election_timeout(), state)
    state = %{state | term: current_term+1, responses: %{}, leader: :undefined}
    state = set_metadata(me, state)
    _ = request_votes(state)
    state
  end

  defp set_metadata(candidate_id, %State{me: me, term: term}=state) do
    state = %{state | voted_for: candidate_id}
    :ok = Log.set_metadata(me, candidate_id, term)
    state
  end

  defp election_timeout() do
    :crypto.rand_uniform(@election_timeout_min, @election_timeout_max)
  end

  defp reset_timer(timeout, %State{timer: timer}=state) do
    if timer do
      _ = Process.cancel_timer(timer)
    end

    new_timer = Process.send_after(self(), :timeout, timeout)
    %{state | timer: new_timer}
  end
end
