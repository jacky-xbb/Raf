defmodule Raf.Log do
  @moduledoc """
  A log is made up of a file header and entries. The header contains file
  metadata and is written once at file creation. Each entry is a binary
  of arbitrary size containing header information and is followed by a trailer.
  The formats of the file header and entries are described below.

  ## file header Format
  ```
  <<version::8>>
  ```


  ## Entry Format
  ```
  <<sha1::binary-size(20), type::8, term::64, index::64, data_size::32, data::binary>>
  ```

  > sha1 - hash of the rest of the entry,
  type - ?CONFIG | ?OP
  term - The term of the entry
  index - The log index of the entry
  data_size - The size of data in bytes
  data - data encoded with term_to_binary/1

  After each log entry a trailer is written. The trailer is used for
  detecting incomplete/corrupted writes, pointing to the latest config and
  traversing the log file backwards.

  ## trailer Format
  ```
  <<crc:32, config_start:64, entry_start:64, @magic:64>>
  ```

  > crc - checksum, computed with :erlang.crc32/1, of the rest of the trailer
    config_start - file location of last seen config,
    entry_start - file location of the start of this entry
    @magic - magic number marking the end of the trailer.
            A fully consistent log should always have
            the following magic number as the last 8 bytes:
            <<"\xFE\xED\xFE\xED\xFE\xED\xFE\xED">>

  """
  use GenServer
  require Logger

  alias Raf.{
    Log,
    Config,
    Opts
  }

  @max_hints 1000

  # Entry Types
  @noop 0
  @config 1
  @op 2

  @magic "\xFE\xED\xFE\xED\xFE\xED\xFE\xED"
  @magic_size 8
  @header_size 41
  @trailer_size 28
  @file_header_size 1
  @read_block_size 1048576 # 1MB
  @latest_version 1

  @type index() :: non_neg_integer()
  @type offset() :: non_neg_integer()

  defmodule State do
    @type index() :: non_neg_integer()
    @type offset() :: non_neg_integer()

    defstruct [:logfile, :version, :meta_filename, :config, :config_loc, :meta, :last_entry,
               :hints, index: 0, write_location: 0, hint_prunes: 0, seek_counts: %{}]

    @type t :: %__MODULE__{
      logfile: :file.io_device(),
      version: non_neg_integer(),
      meta_filename: String.t(),
      write_location: non_neg_integer(),
      config: %Config{},
      config_loc: offset(),
      meta: %Log.Meta{},
      last_entry: %Log.Entry{},
      index: index(),
      hints: :ets.tid(),
      hint_prunes: non_neg_integer()
    }
  end

  # Api

  def entry_to_binary(%Log.Entry{type: :noop, term: term, index: index, cmd: :noop}) do
    entry_to_binary(@noop, term, index, :noop)
  end
  def entry_to_binary(%Log.Entry{type: :config, term: term, index: index, cmd: data}) do
    entry_to_binary(@config, term, index, data)
  end
  def entry_to_binary(%Log.Entry{type: :op, term: term, index: index, cmd: data}) do
    entry_to_binary(@op, term, index, data)
  end

  def entry_to_binary(type, term, index, data) do
    bin_data = :erlang.term_to_binary(data)
    b0 = <<type::8, term::64, index::64, (byte_size(bin_data))::32, bin_data::binary>>
    sha1 = :crypto.hash(:sha, b0)
    <<sha1::binary-size(20), b0::binary>>
  end

  # We want to crash on badmatch here if if our log is corrupt
  # TODO: Allow an operator to repair the log by truncating at that point
  # or repair each entry 1 by 1 by consulting a good log.
  def binary_to_entry(<<sha1::binary-size(20), type::8, term::64, index::64, size::32, data::binary>>) do
    ^sha1 = :crypto.hash(:sha, <<type::8, term::64, index::64, size::32, data::binary>>)
    binary_to_entry(type, term, index, data)
  end

  def binary_to_entry(@noop, term, index, _data) do
    %Log.Entry{type: :noop, term: term, index: index, cmd: :noop}
  end

  def binary_to_entry(@config, term, index, data) do
    %Log.Entry{type: :config, term: term, index: index, cmd: :erlang.binary_to_term(data)}
  end

  def binary_to_entry(@op, term, index, data) do
    %Log.Entry{type: :op, term: term, index: index, cmd: :erlang.binary_to_term(data)}
  end

  @spec start_link([...]) :: :ignore | {:error, any} | {:ok, pid}
  def start_link([peer, opts]) do
    GenServer.start_link(__MODULE__, {peer, opts}, name: log_name(peer))
  end

  def stop(peer) do
    GenServer.cast(log_name(peer), :stop)
  end

  @doc """
  Gets called in the follower state only and will only
  truncate the log if entries don't match. It never truncates and re-writes
  committed entries as this violates the safety of the RAFT protocol.
  """
  def check_and_append(peer, entries, index) do
    GenServer.call(log_name(peer), {:check_and_append, entries, index})
  end

  @doc """
  Gets called in the leader state only, and assumes a truncated log.
  """
  def append(peer, entries) do
    GenServer.call(log_name(peer), {:append, entries})
  end

  def get_config(peer) do
    GenServer.call(log_name(peer), :get_config)
  end

  def get_last_index(peer) do
    GenServer.call(log_name(peer), :get_last_index)
  end

  def get_last_entry(peer) do
    GenServer.call(log_name(peer), :get_last_entry)
  end

  def get_last_term(peer) do
    case get_last_entry(peer) do
      {:ok, %Log.Entry{term: term}} ->
        term
      {:ok, :not_found} ->
        0
    end
  end

  def get_metadata(peer) do
    GenServer.call(log_name(peer), :get_metadata)
  end

  def set_metadata(peer, voted_for, term) do
    GenServer.call(log_name(peer), {:set_metadata, voted_for, term})
  end

  def get_entry(peer, index) do
    GenServer.call(log_name(peer), {:get_entry, index})
  end

  def get_term(peer, index) do
    case get_entry(peer, index) do
      {:ok, %Log.Entry{term: term}} ->
        term
      {:ok, :not_found} ->
        0
    end
  end

  # gen_server callbacks

  def init({name, %Opts{logdir: log_dir}}) do
    log_name = log_dir <> "/raf_" <> Atom.to_string(name) <> ".log"
    meta_name = log_dir <> "/raf_" <> Atom.to_string(name) <> ".meta"
    {:ok, log_file} = :file.open(log_name, [:append, :read, :binary, :raw])
    {:ok, %File.Stat{size: size}} = File.stat(log_name)
    {:ok, meta} = read_metadata(meta_name, size)
    {config_loc, config, _term, index, write_location, version} = init_file(log_file, size)
    last_entry = find_last_entry(log_file, write_location)
    hints_table = String.to_atom("raf_hints_"  <>  Atom.to_string(name))
    {:ok, %State{logfile: log_file,
                version: version,
                meta_filename: meta_name,
                write_location: write_location,
                index: index,
                meta: meta,
                config: config,
                config_loc: config_loc,
                last_entry: last_entry,
                hints: :ets.new(hints_table, [:named_table, :ordered_set, :protected])}}

  end

  def format_status(_, [_, state]) do
    [data: [{'State', "Current state data: '#{inspect state}'"}]]
  end

  # Leader Append. entries do NOT have Indexes, as they are unlogged entries as a
  # result of client operations. Appends are based on the current index of the log.
  # Just append to the next location in the log for each entry.
  def handle_call({:append, entries}, _from, %State{logfile: file}=state) do
    new_state = append_entries(file, entries, state)
    index = new_state.index
    {:reply, {:ok, index}, new_state}
  end

  def handle_call(:get_config, _from, %State{config: config}=state) do
    {:reply, config, state}
  end

  def handle_call(:get_last_entry, _from, %State{last_entry: :undefined}=state) do
    {:reply, {:ok, :not_found}, state}
  end
  def handle_call(:get_last_entry, _from, %State{last_entry: last_entry}=state) do
    {:reply, {:ok, last_entry}, state}
  end

  def handle_call(:get_last_index, _from, %State{index: index}=state) do
    {:reply, index, state}
  end

  def handle_call(:get_metadata, _, %State{meta: meta}=state) do
    {:reply, meta, state}
  end

  def handle_call({:set_metadata, voted_for, term}, _, %State{meta_filename: name}=s) do
    meta = %Raf.Log.Meta{voted_for: voted_for, term: term}
    :ok = write_metadata(name, meta)
    {:reply, :ok, %{s | meta: meta}}
  end

  # Follower append. Logs may not match. Write the first entry at the given index
  # and reset the current index maintained in #state{}. Note that entries
  # actually contain correct indexes, since they are sent from the leader.
  # Return the last index written.
  def handle_call({:check_and_append, entries, index}, _from, %State{logfile: file, hints: hints}=s) do
    loc0 = closest_forward_offset(hints, index)
    {loc, count} = get_pos(file, loc0, index)
    state = update_counters(count, 0, s)
    new_state = maybe_append(loc, entries, state)
    %State{index: new_index} = new_state
    {:reply, {:ok, new_index}, new_state}
  end

  def handle_call({:get_entry, index}, _from, %State{logfile: file, hints: hints}=state0) do
    loc = closest_forward_offset(hints, index)
    {res, new_state} =
      case find_entry(file, loc, index) do
        {:not_found, count} ->
          state = update_counters(count, 0, state0)
          {:not_found, state}
        {entry, next_loc, count} ->
          prunes = add_hint(hints, index, next_loc)
          state = update_counters(count, prunes, state0)
          {entry, state}
      end
    {:reply, {:ok, res}, new_state}
  end


  def handle_cast(:stop, %State{logfile: file}=state) do
    :ok = :file.close(file)
    {:stop, :normal, state}
  end
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  def code_change(_old_vsn, state, _extra) do
    {:ok, state}
  end

  #
  # Internal Functions
  #

  defp maybe_append(_, [], state) do
    state
  end
  defp maybe_append(:eof, [entry | entries], state) do
    new_state = write_entry(entry, state)
    maybe_append(:eof, entries, new_state)
  end
  defp maybe_append(loc, [entry | entries], state=%State{logfile: file}) do
    %Log.Entry{index: index, term: term} = entry
    case read_entry(file, loc) do
      {:entry, data, new_location} ->
        case binary_to_entry(data) do
          # We already have this entry in the log. Continue.
          %Log.Entry{index: ^index, term: ^term} ->
            maybe_append(new_location, entries, state)
          %Log.Entry{index: ^index, term: _term} ->
            new_state = truncate_and_write(file, loc, entry, state)
            maybe_append(:eof, entries, new_state)
        end
      :eof ->
        new_state = truncate_and_write(file, loc, entry, state)
        maybe_append(:eof, entries, new_state)
    end
  end

  defp truncate_and_write(file, loc, entry, state0) do
    :ok = truncate(file, loc)
    state1 = maybe_reset_config(file, loc, state0)
    state2 = %{state1 | write_location: loc}
    write_entry(entry, state2)
  end

  @spec reset_config(:file.io_device(), non_neg_integer(), %State{}) :: %State{}
  defp reset_config(file, loc, state) do
    case loc do
      @file_header_size ->
        # Empty file, so reset to blank config
        %{state | config_loc: 0, config: %Config{}}
      _ ->
        # Get config from the previous trailer
        trailer_loc = loc - @trailer_size
        {:ok, trailer} = :file.pread(file, trailer_loc, @trailer_size)
        <<crc::32, rest::binary>> = trailer
        # validate checksum, fail fast.
        ^crc = :erlang.crc32(rest)
        <<config_loc::64, _::binary>> = rest
        case config_loc do
          0 ->
            %{state | config_loc: 0, config: %Config{}}
          _ ->
            {:ok, config} = read_config(file, config_loc)
            %{state | config_loc: config_loc, config: config}
        end
    end
  end

  @spec maybe_reset_config(:file.io_device(), non_neg_integer(), %State{}) :: %State{}
  defp maybe_reset_config(file, loc, %State{config_loc: config_loc}=state) do
    case config_loc >= loc do
      true ->
        reset_config(file, loc, state)
      false ->
        state
    end
  end


  defp init_file(file, 0) do
    {:ok, loc} = write_file_header(file)
    {0, %Config{}, 0, 0, loc, @latest_version}
  end
  defp init_file(file, size) do
    case repair_file(file, size) do
      {:ok, config_loc, term, index, write_loc} ->
        {:ok, version} = read_file_header(file)
        {:ok, config} = read_config(file, config_loc)
        {config_loc, config, term, index, write_loc, version}
      :empty_file ->
        {:ok, loc} = write_file_header(file)
        {0, %Config{}, 0, 0, loc, @latest_version}
    end
  end

  defp read_file_header(file) do
    {:ok, <<version::8>>} = :file.pread(file, 0, @file_header_size)
    {:ok, version}
  end

  defp write_file_header(file) do
    :ok = :file.write(file, <<@latest_version::8>>)
    {:ok, @file_header_size}
  end

  defp read_config(file, loc) do
    {:entry, data, _} = read_entry(file, loc)
    %Log.Entry{type: :config, cmd: config} = binary_to_entry(data)
    {:ok, config}
  end

  defp find_last_entry(_file, write_location) when write_location <= @file_header_size do
    :undefined
  end
  defp find_last_entry(file, write_location) do
    {:ok, <<_::32, _::64, entry_start::64, _::binary>>} =
      :file.pread(file, write_location - @trailer_size, @trailer_size)
    {:entry, entry, _} = read_entry(file, entry_start)
    binary_to_entry(entry)
  end

  defp make_trailer(entry_start, config_start) do
    t = <<config_start::64, entry_start::64, @magic::binary >>
    crc = :erlang.crc32(t)
    <<crc::32, t::binary>>
  end

  defp append_entries(file, entries, state) do
    new_state = List.foldl(entries, state, &append_entry/2)
    :ok = :file.sync(file)
    new_state
  end

  # Append an entry at the next location in the log. The entry does not yet have an
  # index, so add one.
  defp append_entry(%Log.Entry{}=entry, %State{index: index}=state) do
      new_index = index + 1
      new_entry = %{entry | index: new_index}
      write_entry(new_entry, state)
  end

  # Precondition: each entry must have an index at this point.
  defp write_entry(entry, %State{}=state) do
    %Log.Entry{index: index, type: type, cmd: cmd} = entry
    %State{write_location: loc, config: config, config_loc: config_loc, logfile: file} = state
    bin_entry = entry_to_binary(entry)
    {new_config_loc, new_config} =
        maybe_update_config(type, loc, cmd, config_loc, config)
    trailer = make_trailer(loc, new_config_loc)
    :ok = :file.write(file, <<bin_entry::binary, trailer::binary>>)
    new_loc = loc + byte_size(bin_entry) + @trailer_size
    %{state | index: index,
              config: new_config,
              write_location: new_loc,
              config_loc: new_config_loc,
              last_entry: entry}
  end

  defp maybe_update_config(:config, new_config_loc, new_config, _, _) do
    {new_config_loc, new_config}
  end
  defp maybe_update_config(_type, _, _, cur_config_loc, cur_config) do
    {cur_config_loc, cur_config}
  end

  # This function reads the next entry from the log at the given location
  # and returns {:entry, entry, new_location}. If the end of file has been reached,
  # return eof to the client. Errors are fail-fast.
  @spec read_entry(:file.io_device(), non_neg_integer()) ::
      {:entry, binary(), non_neg_integer()} | {:skip, non_neg_integer()} | :eof
  defp read_entry(file, location) do
    case :file.pread(file, location, @header_size) do
      {:ok, <<_sha1::binary-size(20), _type::8, _term::64, _index::64, _data_size::32>>=header} ->
        read_data(file, location + @header_size, header)
      :eof ->
        :eof
    end
  end

  # TODO: Write to a tmp file then rename so the write is always atomic and the
  # metadata file cannot become partially written.
  defp write_metadata(file_name, meta) do
    :ok = :file.write_file(file_name, :erlang.term_to_binary(meta))
  end

  defp read_metadata(file_name, file_size) do
    case :file.read_file(file_name) do
      {:ok, bin} ->
        {:ok, :erlang.binary_to_term(bin)}
      {:error, :enoent} when file_size <= @file_header_size ->
        {:ok, %Log.Meta{}}
      {:error, reason} ->
        Logger.error("Failed to open metadata file: #{inspect(file_name)}. reason = #{inspect(reason)}")
        {:ok, %Log.Meta{}}
    end
  end

  defp truncate(file, pos) do
    {:ok, _} = :file.position(file, pos)
    :file.truncate(file)
  end

  defp maybe_truncate(file, truncate_at, file_size) do
    case truncate_at < file_size do
      true ->
        :ok = truncate(file, truncate_at)
      false ->
        :ok
    end
  end

  defp repair_file(file, size) do
    case scan_for_trailer(file, size) do
      {:ok, config_start, entry_start, truncate_at} ->
        maybe_truncate(file, truncate_at, size)
        {:entry, data, _} = read_entry(file, entry_start)
        %Log.Entry{term: term, index: index} = binary_to_entry(data)
        {:ok, config_start, term, index, truncate_at}
      :not_found ->
        Logger.warn("NOT FOUND: size = #{inspect(size)}")
        :ok = truncate(file, 0)
        :empty_file
    end
  end

  defp scan_for_trailer(file, loc) do
    case find_magic_number(file, loc) do
      {:ok, magic_loc} ->
        case :file.pread(file, magic_loc - (@trailer_size-@magic_size), @trailer_size) do
          {:ok, <<crc::32, config_start::64, entry_start::64, _::binary >>} ->
            case :erlang.crc32(<<config_start::64, entry_start::64, @magic::binary >>) do
              ^crc ->
                {:ok, config_start, entry_start, magic_loc + 8}
              _ ->
                scan_for_trailer(file, magic_loc)
            end
          :eof ->
            :not_found
        end
      :not_found ->
        :not_found
    end
  end

  defp read_block(file, loc) do
    case loc < @read_block_size do
      true ->
        {:ok, buffer} = :file.pread(file, 0, loc)
        {buffer, 0}
      false ->
        start = loc - @read_block_size
        {:ok, buffer} = :file.pread(file, start, @read_block_size)
        {buffer, start}
    end
  end

  # Continuously read blocks from the file and search backwards until the
  # magic number is found or we reach the beginning of the file.
  defp find_magic_number(file, loc) do
    {block, start} = read_block(file, loc)
    case find_last_magic_number_in_block(block) do
      {:ok, offset} ->
        Logger.info("Magic Number found at #{inspect(start+offset)}")
        {:ok, start+offset}
      :not_found ->
        case start do
          0 ->
            :not_found
          _ ->
            # Ensure we search the overlapping 8 bytes between blocks
            find_magic_number(file, start+8)
        end
    end
  end

  @spec find_last_magic_number_in_block(binary()) :: {:ok, non_neg_integer()} | :not_found
  defp find_last_magic_number_in_block(block) do
    case :string.rstr(block, @magic) do
      0 ->
        :not_found
      index ->
        # We want the 0 based binary offset, not the 1 based list offset.
        {:ok, index - 1}
    end
  end

defp get_pos(file, loc, index) do
  get_pos(file, loc, index, 0)
end

defp get_pos(file, loc, index, count) do
  case :file.pread(file, loc, @header_size) do
    {:ok, <<_sha1::binary-size(20), _type::8, _term::64, ^index::64, _data_size::32>>} ->
      {loc, count}
    {:ok, <<_::binary-size(37), data_size::32>>} ->
      get_pos(file, next_entry_loc(loc, data_size), index, count+1)
    :eof ->
      {:eof, count}
  end
end

  # Find an entry at the given index in a file. Search forward from loc.
  defp find_entry(file, loc, index) do
    find_entry(file, loc, index, 0)
  end

  defp find_entry(file, loc, index, count) do
    case :file.pread(file, loc, @header_size) do
      {:ok, <<_sha1::binary-size(20), _type::8, _term::64, ^index::64, _data_size::32>>=header} ->
        case read_data(file, loc + @header_size, header) do
          {:entry, entry, _} ->
            {binary_to_entry(entry), loc, count}
          :eof ->
            # This should only occur if the entry is currently being written.
            {:not_found, count}
        end
      {:ok, <<_::binary-size(37), data_size::32>>} ->
        next_loc = next_entry_loc(loc, data_size)
        find_entry(file, next_loc, index, count+1)
      :eof ->
        {:not_found, count}
    end
  end

  defp next_entry_loc(loc, data_size) do
    loc + @header_size + data_size + @trailer_size
  end

  @spec read_data(:file.io_device(), non_neg_integer(), binary()) ::
        {:entry, binary(), non_neg_integer()} | :eof
  defp read_data(file, location, <<sha1::binary-size(20), type::8, term::64, index::64, size::32>>=h) do
    case :file.pread(file, location, size) do
      {:ok, data} ->
        # Fail-fast Integrity check.
        # TODO: Offer user repair options?
        ^sha1 = :crypto.hash(:sha, <<type::8, term::64, index::64, size::32, data::binary>>)
        new_location = location + size + @trailer_size
        {:entry, <<h::binary, data::binary>>, new_location}
      :eof ->
        :eof
    end
  end

  # defp log_name({name, _node}) do
  #   String.to_atom(Atom.to_string(name) <> "_log")
  # end
  # defp log_name(me) do
  #   String.to_atom(Atom.to_string(me) <> "_log")
  # end

  defp log_name({name, _}), do: log_name(name)
  defp log_name(name), do: :"#{name}_log"

  @spec update_counters(offset(), non_neg_integer(), %State{}) :: %State{}
  defp update_counters(distance, prunes, %State{hint_prunes: prunes0, seek_counts: dict0}=state) do
    dict = Map.update(dict0, distance, 1, &(&1+1))
    %{state | hint_prunes: prunes0 + prunes, seek_counts: dict}
  end

  @spec closest_forward_offset(:ets.tid(), index()) :: offset()
  defp closest_forward_offset(hints, index) do
    case :ets.prev(hints, index) do
        :"$end_of_table" ->
            @file_header_size
        key ->
            [{^key, loc0}] = :ets.lookup(hints, key)
            loc0
    end
  end

  @spec add_hint(:ets.tid(), index(), offset()) :: non_neg_integer()
  defp add_hint(hints, index, loc) do
    {:size, size} = List.keyfind(:ets.info(hints), :size, 0)
    case size >= @max_hints do
      true ->
        delete_hints(hints)
        true = :ets.insert(hints, {index, loc})
        1
      false ->
        true = :ets.insert(hints, {index, loc})
        0
    end
  end

  # Delete every 10th hint
  defp delete_hints(hints) do
    l = :ets.tab2list(hints)
    {_, to_delete} = List.foldl(l, {0, []}, fn ({index, _}, {count, deleted}) ->
      case rem(count, 10) === 0 do
        true ->
          {count+1, [index | deleted]}
        false ->
          {count+1, deleted}
      end
    end)

    Enum.each(to_delete, fn index ->
      true = :ets.delete(hints, index)
    end)
  end

end
