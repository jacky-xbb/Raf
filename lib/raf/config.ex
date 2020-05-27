defmodule Raf.Config do
  defstruct [state: :blank, oldservers: [], newservers: []]

  @type t :: %__MODULE__{
    # The :blank configuration specifies no servers. Servers that are new to the
    # cluster and have empty logs start in this state.
    # The :stable configuration specifies a single list of servers: a quorum
    # requires any majority of oldservers.
    # The :staging configuration specifies two lists of servers: a quorum requires
    # any majority of oldservers, but the newservers also receive log entries.
    # The :transitional configuration specifies two lists of servers: a quorum requires
    # any majority of oldservers and any majority of the newservers.
    state: :blank | :stable | :staging | :transitional,
    oldservers: list(),
    newservers: list()
  }
end
