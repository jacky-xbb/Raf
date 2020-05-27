defmodule Raf.Opts do
  defstruct [:logdir, state_machine: :rafter_backend_echo]

  @type t :: %__MODULE__{
    state_machine: atom(),
    logdir: String.t()
  }

end
