defmodule LogCrate do
  alias __MODULE__
  alias __MODULE__.Config
  alias __MODULE__.IndexEntry
  alias __MODULE__.Reader
  alias __MODULE__.Writer
  require Logger
  use GenServer

  @type msg_id :: integer
  @type value :: binary

  @opaque t :: %__MODULE__{
    config:      Config.t,
    next_msg_id: integer,
  }
  defstruct config:            nil,
            index:             nil,
            next_msg_id:       nil,
            in_flight_appends: nil,
            writer:            nil

  @spec create(binary) :: pid | {:error, any}
  def create(dir) do
    config = %Config{
      dir: dir,
    }
    GenServer.start_link(__MODULE__, {:create, config})
  end

  @spec close(pid) :: :ok
  def close(crate_pid) do
    GenServer.call(crate_pid, :stop)
  end

  @spec empty?(pid) :: boolean
  def empty?(crate_pid) do
    GenServer.call(crate_pid, :empty?)
  end

  @spec append(pid, value) :: msg_id | {:error, any}
  def append(crate_pid, value) do
    GenServer.call(crate_pid, {:append, value})
  end

  @spec read(pid, msg_id) :: value | :not_found | {:error, any}
  def read(crate_pid, msg_id) do
    GenServer.call(crate_pid, {:read, msg_id})
  end


  #
  # GenServer callbacks
  #

  def init({init_mode, config}) do
    crate = %LogCrate{
      config:            config,
      index:             HashDict.new,
      next_msg_id:       0,
      in_flight_appends: :queue.new,
    }
    {:ok, {init_mode, crate}, 0}
  end

  # initialization that creates a new crate
  def handle_info(:timeout, {:create, %LogCrate{} = crate}) do
    File.mkdir_p!(crate.config.dir)
    {:ok, writer} = Writer.start_link(self, crate.config)
    crate = %{crate | writer: writer}
    {:noreply, crate}
  end

  def handle_call(:empty?, _from, %LogCrate{} = crate) do
    result = true
    {:reply, result, crate}
  end

  def handle_call({:append, value}, from, %LogCrate{} = crate) do
    msg_id = crate.next_msg_id
    Writer.append(crate.writer, msg_id, value)
    new_queue = :queue.in(from, crate.in_flight_appends)
    crate = %{crate | next_msg_id: crate.next_msg_id + 1, in_flight_appends: new_queue}

    {:noreply, crate}
  end

  def handle_call({:read, msg_id}, from, %LogCrate{} = crate) do
    case Dict.get(crate.index, msg_id) do
      nil ->
        GenServer.reply(from, :not_found)

      entry ->
        Reader.start_link(from, crate.config.dir, 0, entry.pos, entry.size)
    end

    {:noreply, crate}
  end


  # event from the Writer when a message has been committed to disk
  def handle_cast({:did_append, msg_id, pos, size}, %LogCrate{} = crate) do
    crate = case :queue.out(crate.in_flight_appends) do
      {{:value, caller}, new_queue} ->
        GenServer.reply(caller, msg_id)
        new_index = Dict.put(crate.index, msg_id, IndexEntry.new(pos, size))
        %{crate | in_flight_appends: new_queue, index: new_index}

      {:empty, _new_queue} ->
        Logger.error("BUG LogCrate got commit from writer but in_flight_appends queue is empty. msg_id=#{msg_id} pos=#{pos} size=#{size}")
        throw(:bug)
    end

    {:noreply, crate}
  end

  # event from the Writer when it has rolled over to a new segment
  def handle_cast({:roll, _new_segment_id}, %LogCrate{} = crate) do
    {:noreply, crate}
  end
end
