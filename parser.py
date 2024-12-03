import os
import networkx as nx
import polars as pl

def parse_ztm_stops_data_add_nodes_layered(
    G: nx.Graph, stops_data: pl.DataFrame
) -> nx.Graph:
    """
    Parse the ZTM dataset and add nodes to the graph. The nodes are bus stops that are contained in stops.txt file.
    """
    for row in stops_data.rows(named=True):
        G.add_node(
            row["stop_id"],
            pos=(row["stop_lon"], row["stop_lat"]),
            name=row["stop_name"],
        )
    return G


def time_to_seconds(time_str):

    h, m, s = map(int, time_str.split(":"))
    return h * 3600 + m * 60 + s


def parse_ztm_stops_data_add_edges_layered(
    graphs: list,
    stop_times_data: pl.DataFrame,
    trips_data: pl.DataFrame,
    routes_data: pl.DataFrame,
    stops_data: pl.DataFrame,
    n_rows: int,
) -> None:
    """
    Create a single datframe with stops, trips and routes. Multiple trips can be of the same route. Each trip has a sequence of stops. We want to draw paths that represent the trips.
    """

    # Merge stop_times and trips based on trip_id

    stop_times_trips_merged = stop_times_data.join(trips_data, on="trip_id")

    # Merge stop_times_trips_merged with routes based on route_id

    stop_times_trips_merged_routes_merged = stop_times_trips_merged.join(
        routes_data, on="route_id"
    )

    # Since there is the n_limit parameter, not all nodes might be used. So we need to filter out the nodes that are not used.

    # stop_times_trips_merged_routes_merged = (
    #     stop_times_trips_merged_routes_merged.filter(
    #         pl.col("stop_id").is_in(list(G.nodes()))
    #     )
    # )

    # Group by trip_id, meaning we iterate over each trip, take stops one by one and create edges between the stops. We convert a path == trip to a graph.
    if n_rows != 0:
        stop_times_trips_merged_routes_merged = (
            stop_times_trips_merged_routes_merged.limit(n_rows)
        )
    # print(stop_times_trips_merged_routes_merged.__len__())

    # print(stop_times_trips_merged_routes_merged.n_unique("route_id"))
    # return None
    stop_times_trips_merged_routes_merged = (
        stop_times_trips_merged_routes_merged.with_columns(
            pl.col("arrival_time")
            .map_elements(time_to_seconds, return_dtype=pl.Int64)
            .alias("arrival_time_seconds")
        )
    )

    for name, groupedData in stop_times_trips_merged_routes_merged.group_by(
        ["pickup_type"]
    ):
        G = nx.Graph()
        for name, data in groupedData.group_by(["trip_id"]):

            unique_stop_ids = data["stop_id"].unique()
            stop_info = stops_data.filter(pl.col("stop_id").is_in(unique_stop_ids))
            G = parse_ztm_stops_data_add_nodes_layered(G, stop_info)

            # Order the stops, to make sure we are creating the correct path
            data = data.sort("stop_sequence")
            for i in range(data.shape[0] - 1):
                G.add_edge(
                    int(data[i]["stop_id"][0]),
                    int(data[i + 1]["stop_id"][0]),
                    start_time=data[i]["arrival_time_seconds"][0],
                    end_time=data[i + 1]["arrival_time_seconds"][0],
                )

        graphs.append(G)

    # Remove nodes with no edges for graph clarity
    # nodes_to_remove = [node for node, degree in dict(G.degree()).items() if degree == 0]
    # G.remove_nodes_from(nodes_to_remove)

    return None


def parse_ztm_stops_data_layered(dataset_path: str, n_rows: int = 0) -> nx.Graph:

    # Load the neccessary data to process stops
    routes_data = pl.read_csv(
        os.path.join(dataset_path, "routes.txt"), separator=",", has_header=True
    )
    stop_times_data = pl.read_csv(
        os.path.join(dataset_path, "stop_times.txt"), separator=",", has_header=True
    )
    stops_data = pl.read_csv(
        os.path.join(dataset_path, "stops.txt"), separator=",", has_header=True
    )
    trips_data = pl.read_csv(
        os.path.join(dataset_path, "trips.txt"), separator=",", has_header=True
    )

    graphs_stops = []
    # graph_stops = parse_ztm_stops_data_add_nodes(graph_stops, stops_data, n_rows)
    parse_ztm_stops_data_add_edges_layered(
        graphs_stops, stop_times_data, trips_data, routes_data, stops_data, n_rows
    )

    return graphs_stops
