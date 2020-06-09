defmodule Raf.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Raf.Worker.start_link(arg)
      # {Raf.Worker, arg}
      Raf.Server.Supervisor,
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Raf.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
