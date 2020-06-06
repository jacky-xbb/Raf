defmodule Raf.Msg do

  defmodule AppendEntriesReq do
    defstruct [:term, :from, :prev_log_index, :prev_log_term, :entries,
               :commit_index, :send_clock]

    @type t :: %__MODULE__{
      term: non_neg_integer(),
      from: atom(),
      prev_log_index: non_neg_integer(),
      prev_log_term: non_neg_integer(),
      entries: term(),
      commit_index: non_neg_integer(),

      # This is used during read-only operations
      send_clock: non_neg_integer()
    }
  end


  defmodule AppendEntriesResp do
    defstruct [:from, :term, :index, :send_clock, :success]

    @type t :: %__MODULE__{
      from: atom(),
      term: non_neg_integer(),

      # This field isn't in the raft paper. However, for this implementation
      # it prevents duplicate responses from causing recommits and helps
      # maintain safety. In the raft reference implementation (logcabin)
      # they cancel the in flight RPC's instead. That's difficult
      # to do correctly(without races) in erlang with asynchronous
      # messaging and mailboxes.
      index: non_neg_integer(),

      # This is used during read-only operations
      send_clock: non_neg_integer(),

      success: boolean()
    }
  end

  defmodule RequestVoteReq do
    defstruct [:term, :from, :last_log_index, :last_log_term]

    @type t :: %__MODULE__{
      term: non_neg_integer(),
      from: atom(),
      last_log_index: non_neg_integer(),
      last_log_term: non_neg_integer()
    }
  end

  defmodule RequestVoteResp do
    defstruct [:from, :term, :success]

    @type t :: %__MODULE__{
      from: atom(),
      term: non_neg_integer(),
      success: boolean()
    }
  end
end
