defmodule LogCrate do
  alias __MODULE__
  alias __MODULE__.Config
  use GenServer

  @type msg_id :: integer
  @type value :: binary

  @opaque t :: %__MODULE__{
    config: Config.t
  }
  defstruct config: nil

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


  #
  # GenServer callbacks
  #

  def init({init_mode, config}) do
    crate = %LogCrate{config: config}
    {:ok, {init_mode, crate}, 0}
  end

  def handle_info(:timeout, {:create, %LogCrate{} = crate}) do
    File.mkdir_p!(crate.config.dir)

    {:noreply, crate}
  end

  def handle_call(:empty?, _from, %LogCrate{} = crate) do
    result = true
    {:reply, result, crate}
  end
end
