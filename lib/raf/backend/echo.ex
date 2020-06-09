defmodule Raf.Backend.Echo do
  @behaviour Raf.Backend

  def init(_) do
    :ok
  end

  def read(command, state) do
    {{:ok, command}, state}
  end

  def write(command, state) do
    {{:ok, command}, state}
  end

end
