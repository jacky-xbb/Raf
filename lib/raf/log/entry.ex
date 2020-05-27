defmodule Raf.Log.Entry do
  @moduledoc """
  Define a log entry struct.
  """
  defstruct [:type, :term, :index, :cmd]

  @type t :: %__MODULE__{
    type: :noop | :config | :op,
    term: non_neg_integer(),
    index: non_neg_integer(),
    cmd: term()
  }

end
