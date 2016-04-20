# read the file from the bottom to the TOP !!

defmodule Mix.Tasks.WorkerChunk do
  @moduledoc """
  Constraints:
  - Big file
  - Too many records for not doing things on a single core
  - Too many records to launch simultaniously a process for each record
  - Too many records to launch simultaniously a process for each chunk (you want a fixed number of parallel process)
  - long calculation (slower than the IO rate of file chunks)

  Process : 
  - Stream file every 1MB, showing a stream of lines (browse every Bytes, and spawn next reduce if byte = ?\n)
  - Iterate the list chunk of 20_000 element on the stream if you iterate the (i*20_000) em element of the source enum
  - For this 20000 elems, queue line and calculation on workers (roundrobin)
  - Then send the "get" message for all workers which will be queued after the calculations and so lock 
    the current process until all lines are computed and get the final accumulator
  - So when you reduce the resulting stream (with the final "sum") :
      - you use all the cores of your CPU
      - you never handle more than 20_000 lines concurrently 
      - you never handle more than 100 parallel process simultaniously
      - you handle the "clever" (business) part (line size calculation) record per record (per line here)
      - you read the file in an optimal way : big file chunk of 1MO (never more than 1MO in RAM)
  """
  def run([]) do
    :timer.tc(fn->
      IO.puts @moduledoc
      nb_worker = 100
      workers = 1..nb_worker |> Enum.map(fn _-> {:ok,pid} = Agent.start_link(fn-> 0 end); pid end)
      stream = File.stream!("sample_data.csv",[read_ahead: 1_000_000], :line)
      |> Stream.zip(Stream.cycle(workers))
      |> Stream.chunk(20000,20000)
      |> Stream.map(fn chunk->
        Enum.map(chunk,fn {line,worker}->
          Agent.cast(worker,fn count->
            count + (line |> String.upcase |> to_char_list |> Enum.count)
          end)
        end)
        workers |> Enum.map(fn worker->Agent.get(worker, &(&1),:infinity) end) |> Enum.sum
      end)
      |> Enum.sum
    end) |> inspect |> IO.puts
  end
end

defmodule Mix.Tasks.Worker do
  @moduledoc """
  Constraints:
  - Big file
  - Too many records for not doing things on a single core
  - Too many records to launch simultaniously a process for each record
  - Too many records to launch simultaniously a process for each chunk (you want a fixed number of parallel process)
  - fast calculation (faster than the IO rate of file chunks), so all the computations can be send in series to workers

  Process : 
  - Stream file every 1MB, showing a stream of lines (browse every Bytes, and spawn next reduce if byte = ?\n)
  - For each line, queue line and calculation on workers (roundrobin)
  - Then send the "get" message for all workers which will be queued after the calculations and so lock 
    the current process until all lines are computed and get the final accumulator
  - So when you reduce the resulting stream (with the final "sum") :
      - you use all the cores of your CPU
      - you never handle more than 100 parallel process simultaniously
      - you handle the "clever" (business) part (line size calculation) record per record (per line here)
      - you read the file in an optimal way : big file chunk of 1MO (never more than 1MO in RAM)

  If the process is too slow, all lines can be in RAM at the same time as message in the worker queues
  """
  def run([]) do
    :timer.tc(fn->
      IO.puts @moduledoc
      nb_worker = 100
      workers = 1..nb_worker |> Enum.map(fn _-> {:ok,pid} = Agent.start_link(fn-> 0 end); pid end)
      stream = File.stream!("sample_data.csv",[read_ahead: 1_000_000], :line)
      |> Stream.zip(Stream.cycle(workers))
      |> Enum.map(fn {line,worker}->
          Agent.cast(worker,fn count->
            count + (line |> String.upcase |> to_char_list |> Enum.count)
          end)
        end)

      workers |> Enum.map(fn worker->Agent.get(worker, &(&1),:infinity) end) |> Enum.sum
    end) |> inspect |> IO.puts
  end
end

defmodule Mix.Tasks.TaskChunk do
  @moduledoc """
  Constraints:
  - Big file
  - Too many records for not doing things on a single core
  - Too many records to launch simultaniously a process for each record
  - But quite a few records to launch simultaniously a process for each chunk

  Process : 
  - Stream file every 1MB, showing a stream of lines (browse every Bytes, and spawn next reduce if byte = ?\n)
  - Iterate the list chunk of 8000 record on the stream if you iterate the (i*8000) em record of the source enum
  - For each chunk, stark a worker to do the processing for each element of the chunk 
    and send back the result to parent process
  - For each worker, get the chunk result
  - So when you reduce the resulting stream (with the final "sum") :
      - you use all the cores of your CPU
      - you handle the "clever" (business) part (line size calculation) record per record (per line here)
      - you read the file in an optimal way : big file chunk of 1MO (never more than 1MO in RAM)

  If there is too many records, it can make the number of parallel processing too big to be performant
  """
  def run([]) do
    IO.puts @moduledoc
    :timer.tc(fn->
      stream = File.stream!("sample_data.csv",[read_ahead: 1_000_000], :line)
      |> Stream.chunk(8000,8000,[])
      |> Enum.map(fn chunk->
        Task.async(fn->
          chunk |> Enum.map(fn line->
            line |> String.upcase |> to_char_list |> Enum.count
          end) |> Enum.count
        end)
      end)
      |> Stream.map(& Task.await(&1,:infinity))
      |> Enum.sum
    end) |> inspect |> IO.puts
  end
end

defmodule Mix.Tasks.Task do
  @moduledoc """
  Constraints:
  - Big file
  - Too many records for not doing things on a single core
  - But quite a few records to to launch simultaniously a process for each record

  Process : 
  - Stream file every 1MB, showing a stream of lines (browse every Bytes, and spawn next reduce if byte = ?\n)
  - Iterate the list chunk of 8000 record on the stream if you iterate the (i*8000) em record of the source enum
  - For each record, stark a worker to do the processing and send back the result to parent process
  - For each worker, get the chunk result
  - So when you reduce the resulting stream (with the final "sum") :
      - you use all the cores of your CPU
      - you handle the "clever" (business) part (line size calculation) record per record (per line here)
      - you read the file in an optimal way : big file chunk of 1MO (never more than 1MO in RAM)

  If there is too many records, it can make the number of parallel processing too big to be performant
  """
  def run([]) do
    IO.puts @moduledoc
    :timer.tc(fn->
      stream = File.stream!("sample_data.csv",[read_ahead: 1_000_000], :line)
      |> Enum.map(fn line->
        Task.async(fn->
          line |> String.upcase |> to_char_list |> Enum.count
        end)
      end)
      |> Stream.map(& Task.await(&1,:infinity))
      |> Enum.sum
    end) |> inspect |> IO.puts
  end
end

defmodule Mix.Tasks.Stream do
  @moduledoc """
  Constraints:
  - Big file
  - Quite a few records so a single Core usage is OK

  Process : 
  - Stream file every 1MB, showing a stream of lines (browse every Bytes, and spawn next reduce if byte = ?\n)
  - Iterate the list chunk of 8000 record on the stream if you iterate the (i*8000) em record of the source enum
  - For each record, do the computation
  """
  def run([]) do
    IO.puts @moduledoc
    :timer.tc(fn->
      stream = File.stream!("sample_data.csv",[read_ahead: 1_000_000], :line)
      |> Stream.map(fn line->
        line |> String.upcase |> to_char_list |> Enum.count
      end)
      |> Enum.sum
    end) |> inspect |> IO.puts
  end
end
