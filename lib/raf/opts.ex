defmodule Raf.Opts do
  defstruct [:logdir, state_machine: Raf.Backend.Echo]

  @type t :: %__MODULE__{
    state_machine: module(),
    logdir: String.t()
  }

end
