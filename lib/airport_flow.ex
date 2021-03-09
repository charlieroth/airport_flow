defmodule AirportFlow do
  @moduledoc """
  Root module

  Current contains all logic
  """
  alias NimbleCSV.RFC4180, as: CSV

  def airports_csv() do
    Application.app_dir(:airport_flow, "/priv/airports.csv")
  end

  def open_airports() do
    # Flow operations run in GenStage stage processes making them concurrent
    airports_csv()
    |> File.stream!()
    # Treat the data source as a producer
    |> Flow.from_enumerable()
    # Flow.map/2 and Flow.filter/2 will act as consumer or producer_consumer
    |> Flow.map(fn row ->
      [row] = CSV.parse_string(row, skip_headers: false)

      %{
        id: Enum.at(row, 0),
        type: Enum.at(row, 2),
        name: Enum.at(row, 3),
        country: Enum.at(row, 8)
      }
    end)
    |> Flow.reject(&(&1.type == "closed"))
    # Add a partition layer to ensure items sharing same :country are sent to
    # same reducer
    |> Flow.partition(key: {:key, :country})
    # Flow.reduce/3 is a producer that distributes batches of items to work on
    |> Flow.reduce(fn -> %{} end, fn item, acc ->
      Map.update(acc, item.country, 1, &(&1 + 1))
    end)
    |> Enum.to_list()
  end
end
