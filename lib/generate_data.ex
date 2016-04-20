defmodule Mix.Tasks.GenerateData do
  def run([size]) do
    Stream.repeatedly(fn->
       Base.encode64(:crypto.rand_bytes(70)) <> "\n"
    end)
    |> Stream.take(String.to_integer(size))
    |> Enum.into(File.stream!("sample_data.csv"))
  end
end
