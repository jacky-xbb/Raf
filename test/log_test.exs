defmodule LogTest do
  use ExUnit.Case, async: false

  alias Raf.{
    Log,
    Config,
    Opts
  }

  @moduletag capture_log: true

  @peer :test


  test "log overwrite" do
    cleanup()

    opts = %Opts{logdir: "/tmp"}
    {:ok, _pid} = Log.start_link([@peer, opts])
    assert_empty()

    # We are appending entry1 as the leader, so it has no index.
    entry1 = %Log.Entry{type: :config, term: 1, index: :undefined, cmd: %Config{state: :stable}}
    assert_leader_append(1, 1, entry1)

    config_loc0 = assert_stable_config()
    entry2 = %Log.Entry{type: :noop, term: 1, index: :undefined, cmd: :noop}
    assert_leader_append(2, 1, entry2)
    config_loc1 = assert_stable_config()
    assert config_loc0 == config_loc1

    # A new leader takes over and this log gets its entry overwritten.
    # In reality index 1 will always be a #config{}, but this validates the
    # test that config gets reset.
    entry = %Log.Entry{type: :noop, term: 2, index: 1, cmd: :noop}
    assert_follower_append(entry)
    assert_blank_config()

    # This peer becomes leader again and appends 2 configs
    entry3 = %Log.Entry{type: :config, term: 3, cmd: %Config{state: :stable}}
    assert_leader_append(2, 3, entry3)
    config_loc2 = assert_stable_config()
    entry4 = %Log.Entry{type: :config, term: 3, cmd: %Config{state: :stable}}
    assert_leader_append(3, 3, entry4)
    config_loc3 = assert_stable_config()
    assert config_loc2 != config_loc3

    # A new leader takes over and truncates the last config
    entry5 = %Log.Entry{type: :noop, term: 4, index: 3, cmd: :noop}
    assert_follower_append(entry5)
    config_loc4 = assert_stable_config()
    assert config_loc2 == config_loc4
    index = Log.get_last_index(@peer)
    assert index == 3
    {:ok, entry6} = Log.get_last_entry(@peer)
    assert entry5 == entry6

    # A new leader takes over and truncates the last stable config
    # New config is at position 0
    entry7 = %Log.Entry{type: :noop, term: 5, index: 2, cmd: :noop}
    assert_follower_append(entry7)
    assert_blank_config()
    index2 = Log.get_last_index(@peer)
    assert index2 == 2
    {:ok, entry8} = Log.get_last_entry(@peer)
    assert entry7 == entry8

    Log.stop(@peer)
  end

  defp assert_leader_append(expected_index, expected_term, entry) do
    {:ok, index} = Log.append(@peer, [entry])
    assert expected_index == index
    {:ok, entry1} = Log.get_entry(@peer, index)
    {:ok, ^entry1} = Log.get_last_entry(@peer)
    ^index = Log.get_last_index(@peer)
    assert entry1.index == expected_index
    assert entry1.term == expected_term
  end

defp assert_follower_append(entry) do
  # Note that follower appends always have indexes since they are sent
  # from the leader who has already written the entry to its log.
  index = entry.index
  {:ok, ^index} = Log.check_and_append(@peer, [entry], index)
  {:ok, entry1} = Log.get_entry(@peer, index)
  assert entry == entry1
end

defp assert_blank_config() do
  config = Log.get_config(@peer)
  assert :blank == config.state
  state = :sys.get_state(logname(@peer))
  assert state.config_loc == 0
end

defp  assert_stable_config() do
  config = Log.get_config(@peer)
  assert :stable == config.state
  state = :sys.get_state(logname(@peer))
  config_loc = state.config_loc
  assert config_loc != 0
  config_loc
end

defp assert_empty() do
  assert {:ok, :not_found} == Log.get_last_entry(@peer)
  assert 0 == Log.get_last_index(@peer)
  assert_blank_config()
end

  defp cleanup() do
    System.cmd("rm", ["-rf", "/tmp/raf_test.log"])
  end

  defp logname({name, _node}) do
    String.to_atom(Atom.to_string(name) <> "_log")
  end
  defp logname(me) do
    String.to_atom(Atom.to_string(me) <> "_log")
  end

end
