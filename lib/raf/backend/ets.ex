defmodule Raf.Backend.Ets do
  require Logger

  defmodule State do
    defstruct [:peer]

    @type t :: %__MODULE__{
      peer: atom() | {atom(), atom()}
    }
  end

  def init(peer) do
    state = %State{peer: peer}
    new_state = stop(state)
    _tid1 = :ets.new(:rafter_backend_ets, [:set, :named_table, :public])
    _tid2 = :ets.new(:rafter_backend_ets_tables, [:set, :named_table, :public])
    new_state
  end

  def stop(state) do
    try do
      :ets.delete(:rafter_backend_ets)
      :ets.delete(:rafter_backend_ets_tables)
    rescue
      _ -> Logger.error("ets delete failed!")
    after
      state
    end
  end

  def read({:get, table, key}, state) do
    val =
      try do
        case :ets.lookup(table, key) do
          [{_key, value}] ->
            {:ok, value}
          [] ->
            {:ok, :not_found}
        end
      rescue
        e -> {:error, e}
      end
    {val, state}
  end

  def read(:list_tables, state) do
    tables =
      for {table} <- :ets.tab2list(:rafter_backend_ets_tables) do
        table
      end
    {{:ok, tables}, state}
  end

  def read({:list_keys, table}, state) do
    val =
      try do
        list_keys(table)
      rescue
        e -> {:error, e}
      end
    {val, state}
  end

  def read(_, state) do
    {{:error, :ets_read_badarg}, state}
  end

  def write({:new, name}, state) do
    val =
      try do
        _tid = :ets.new((name), [:ordered_set, :named_table, :public])
        :ets.insert(:rafter_backend_ets_tables, {name})
        {:ok, name}
        rescue
          e -> {:error, e}
      end
    {val, state}
  end

  def write({:put, table, key, value}, state) do
    val =
      try do
        :ets.insert(table, {key, value})
        {:ok, value}
        rescue
          e -> {:error, e}
      end
    {val, state}
  end

  def write({:delete, table}, state) do
    val =
      try do
        :ets.delete(table)
        :ets.delete(:rafter_backend_ets_tables, table)
        {:ok, true}
      rescue
        e -> {:error, e}
      end
    {val, state}
  end
  def write({:delete, table, key}, state) do
    val =
      try do
        {:ok, :ets.delete(table, key)}
      rescue
        e -> {:error, e}
      end
    {val, state}
  end
  def write(_, state) do
    {{:error, :ets_write_badarg}, state}
  end

  defp list_keys(table) do
    list_keys(:ets.first(table), table, [])
  end

  defp list_keys(:"$end_of_table", _table, keys) do
    {:ok, keys}
  end

  defp list_keys(key, table, keys) do
    list_keys(:ets.next(table, key), table, [key | keys])
  end
end
