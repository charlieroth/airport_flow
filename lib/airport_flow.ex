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
    window = Flow.Window.trigger_every(Flow.Window.global(), 1000)
    # Flow operations run in GenStage stage processes making them concurrent
    airports_csv()
    |> File.stream!()
    # Introduce artificial delay to better understand Flow windows and triggers
    |> Stream.map(fn event ->
      Process.sleep(Enum.random([0, 0, 0, 1]))
      event
    end)
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
    |> Flow.partition(window: window, key: {:key, :country})
    # Flow.group_by/2 is more convenient than Flow.reduce/3 if your only goal
    # is to group elements in the list
    |> Flow.group_by(fn item ->
      item.country
    end)
    |> Flow.on_trigger(fn acc, _partition_info, {_type, _id, trigger} ->
      events =
        acc
        |> Enum.map(fn {country, data} -> {country, Enum.count(data)} end)
        |> IO.inspect(label: inspect(self()))

      case trigger do
        :done -> {events, acc}
        {:every, 1000} -> {[], acc}
      end
    end)
    # |> Flow.map(fn {country, data} ->
    #   {country, Enum.count(data)}
    # end)
    # Take only top 10 by count
    # |> Flow.take_sort(10, fn {_, a}, {_, b} ->
    #   a > b
    # end)
    |> Enum.to_list()

    # |> List.flatten()
  end
end
