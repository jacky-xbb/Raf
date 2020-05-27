defmodule Raf.Log.Meta do
  defstruct [voted_for: nil, term: 0]

  @type peer() :: atom() | {atom(), atom()}

  @type t :: %__MODULE__{
    voted_for: peer(),
    term: non_neg_integer()
  }

end
