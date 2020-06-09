defmodule Raf.Backend do
  @callback init(args :: term()) :: state :: term()
  @callback read(operation :: term(), state :: term()) ::
      {value :: term(), state :: term()}
  @callback write(operation :: term(), state :: term()) ::
      {value :: term(), state :: term()}
end
