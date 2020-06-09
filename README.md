# Raf

## Overview
Raf is an Elixir implementation of the Raft distributed consensus protocol. It provides users with an api for building consistent (2F+1 CP), distributed state machines. Raft protocol is described in the [original
paper](https://raft.github.io/raft.pdf).


## Features
- Leader election
- Log replication
- Configuration changes


## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `raf` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:raf, "~> 0.1.0"}
  ]
end
```

## Example

### Launch 3 nodes

```elixir
$ iex --sname peer1@localhost -S mix
iex(peer1@localhost)1> Raf.start_test_node(:peer1)

$ iex --sname peer2@localhost -S mix
iex(peer2@localhost)1> Raf.start_test_node(:peer2)

$ iex --sname peer3@localhost -S mix
iex(peer3@localhost)1> Raf.start_test_node(:peer3)
```

when peers have been running. You can check peers' status:

```elixir
:sys.get_status(:peer1)
or
:sys.get_state(:peer1)
```

### Config cluster

At this point all the peers are in the `follow` state. In order to get them to communicate we need to define
a cluster configuration. In our case, we'll run on node `peer1`:

```elixir
peers = [{peer1, :peer1@localhost},
        {peer2, :peer2@localhost},
        {peer3, :peer3@localhost}]
Raf.set_config(:peer1, peers)
```

Once election is done. You can see who is the current leader:

```elixir
leader = Raf.get_leader(:peer1)
```

### Write Operations

```elixir
# Create a new ets table
raf.write(:peer1, {:new, sometable})

# Store an erlang term in that table
raf.write(:peer1, {:put, sometable, somekey, someval})

# Delete a term from the table
raf.write(:peer1, {:delete, sometable, somekey})

# Delete a table
raf.write(:peer1, {:delete, sometable})
```

### Read Operations

```elixir
# Read an erlang term
raf.read(:peer1, {get, table, key})

# list tables
raf.read(:peer1, :list_tables)

# list keys
raf.read(:peer1, {:list_keys, table})
```


