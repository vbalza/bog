'''
filename: distance.py
author: Valeria Blaza
email: vbalza@uchicago.edu
description: 
    Computes first and second nearest stations and buoys for a dataframe of buoys.
    This module assumes that the input buoy Points are in WGS84 projection.
source(s): 
    - https://automating-gis-processes.github.io/site/notebooks/L3/nearest-neighbor-faster.html
'''

from pandas.tseries import offsets
import geopandas as gpd
import pandas as pd
import numpy as np
import re
from sklearn.neighbors import BallTree
from utilities import files
from datetime import timedelta

def load_data(fname_buoys, fname_stations):
    '''
    Retrieves and formats buoy and station data into geopandas dfs

    Inputs(s):
        - fname_buoys (str): file path name for buoy data (tsv format)
        - fname_stations (str): file path name for stations data (tsv format)
    '''
    buoys_df = files.load_df(fname_buoys)
    buoys_df.rename(columns={
        'time': 'datetime',
        'buoy_lat': 'latitude',
        'buoy_lon': 'longitude',
        'buoy_id' : 'sensor_id'
    }, inplace=True)

    stations_df = files.load_df(fname_stations)[["station_id", "station_name", \
                                                      "latitude", "longitude", "source"]]

    buoys = gpd.GeoDataFrame(buoys_df, geometry=gpd.points_from_xy(buoys_df.longitude, 
                                                                   buoys_df.latitude, 
                                                                   crs="EPSG:4326"))

    stations = gpd.GeoDataFrame(stations_df, geometry=gpd.points_from_xy(stations_df.longitude,
                                                                         stations_df.latitude,
                                                                         crs="EPSG:4326"))

    return buoys, stations


def get_nearest(query_points, candidate_points, metric="haversine", k=2):
    '''
    Finds first and second nearest station for all source coordinate points 
    (e.g., buoys) from a set of candidate coordinate points (e.g., stations)

    Input(s):
        - query_points (GeoDataFrame): Contains source points (e.g., buoys).
            query_points.size = n
        - candidate_points (GeoDataFrame): Contains candidate points
            (e.g., stations). candidate_points.size = c
        - metric (str or DistanceMetric object): Distance metric to use for
            the tree. Defaults to 'haversine'
        - k (int): Number of closest points to query for each point in
            left_gdf

    Output(s):
        - closest_points (list of GeoDataFrame): Contains a list of 
            GeoDataFrames with the jth gdf representing the (j+1)th nearest
            candidate points. closest_points[j]
    '''
    # Convert GeoDataFrames to np.ndarrays
    query_geometry_name = query_points.geometry.name
    candidate_geometry_name = candidate_points.geometry.name
    candidate_points = candidate_points.copy().reset_index(drop=True)

    # Parse coordinates from points and insert them into a numpy array as radians
    query_array = np.array(query_points[query_geometry_name].apply(
        lambda geom: (geom.x * np.pi / 180, geom.y * np.pi / 180)
    ).to_list())
    candidate_array = np.array(candidate_points[candidate_geometry_name].apply(
        lambda geom: (geom.x * np.pi / 180, geom.y * np.pi / 180)
    ).to_list())

    # Create tree from the candidate points
    c = candidate_points.shape[0]
    n = query_points.shape[0]
    max_k = min(c, k)  # c >= k for balltree
    if c > 0:
        tree = BallTree(candidate_array, leaf_size=15, metric=metric)
        distances, indices = tree.query(query_array, k=max_k)
    # distances = distances.transpose()
    # indices = indices.transpose()
    # distances and indices and np.ndarrays of shape (n, max_k) where 
    # indices[i, j] and distances[i, j] are the respective index and distance
    # in radians of the jth closest element to query_array[i].
    # Create closest points dataframes with 'distance' column
    closest_points = []
    earth_radius = 6371 # in kilometers
    for j in range(max_k):
        jth_closest_points = candidate_points.loc[indices[:,j]]
        jth_closest_points.reset_index(drop=True, inplace=True)
        jth_closest_points["distance"] = distances[:,j] * earth_radius
        closest_points.append(jth_closest_points)
    # fill in absent closest neighbors with nans, if any
    for j in range(max_k, k):
        jth_closest_points = pd.DataFrame(np.nan, index=query_points.index, columns=candidate_points.columns) 
        jth_closest_points["distance"] = np.nan
        closest_points.append(jth_closest_points)
    return closest_points


def find_nearest_buoys(buoys, metric='haversine', k=2,  max_temporal_distance=180):
    '''
    Given buoy dataframe, finds first and second buoy neighbors within a 
    time range of +/- 10 days

    Input(s):
        - buoys (gdf): Contains source points (e.g., buoys)
        - metric (str or DistanceMetric object): Distance metric 
            to use for the tree. For a two-dimensional vector space (lat/lon), 
            a "haversine" distance metric is assumed
        - max_temporal_distance (int): Max distance from buoy to consider a
            point a neighbor, in minutes. Default 60.
    Output(s):
        - buoys (gdf): Contains rows corresponding to unique buoy, 
            timestamp data with corresponding first and second distinct nearest buoys.
            If no neighbors are 
    '''
    buoys['datetime'] = pd.to_datetime(buoys['datetime'])
    updated_rows = []
    for index, row in buoys.iterrows():
        row_df = row.to_frame().T
        row_gdf = gpd.GeoDataFrame(row_df, geometry=gpd.points_from_xy(row_df['longitude'], 
                                                                row_df['latitude'], 
                                                                crs="EPSG:4326"))
        # limit search to unique buoys within max temporal distance
        buoy_ids_used = set(row_gdf['sensor_id'])
        id_mask = buoys["sensor_id"].isin(buoy_ids_used)
        candidate_buoys = buoys[id_mask]
        start_time = row.datetime - timedelta(minutes=max_temporal_distance)
        end_time = row.datetime + timedelta(minutes=max_temporal_distance)
        time_mask = ((candidate_buoys['datetime'] > start_time) & 
                (candidate_buoys['datetime'] <= end_time))
        candidate_buoys = candidate_buoys.loc[time_mask].reset_index(drop=True)
        # get the initial closest points not filtered for unique buoys
        closest_points = get_nearest(row_gdf, candidate_buoys, metric, k)

        j,  offset = 0, 0
        neighbor_dfs = [row_gdf]
        # Look through the closest points data. Reformat column names to
        # provide context. If a closest point is from an already used buoy,
        # filter it from candidate buoys, and run find nearest neighbors
        # again for k minus the number of closest points we have already used.
        # Offset is used to keep track of this since closest points will be
        # size k-j, we'll want to index it from j-offset. np.nan means no
        # further candidate points were available so no use in searching again
        while j < k:
            jth_closest_points = closest_points[j-offset]
            buoy_id = jth_closest_points.iloc[0, 0]
            jth_closest_points.columns = [
                str(col) + "_nearest_buoy_{}".format(j+1) 
                for col in jth_closest_points.columns
            ]
            if buoy_id != np.nan and buoy_id in buoy_ids_used:
                id_mask = candidate_buoys["sensor_id"].isin(buoy_ids_used)
                candidate_buoys = candidate_buoys[~id_mask]
                offset = j + offset
                closest_points = get_nearest(row_gdf, candidate_buoys, metric, k-j)
            else:
                neighbor_dfs.append(jth_closest_points)
                buoy_ids_used.add(buoy_id)
                j += 1
        updated_row = pd.concat(neighbor_dfs, axis='columns')
        updated_rows.append(updated_row)
    return pd.concat(updated_rows)


def go(fname_buoys, fname_stations="stations/stations.tsv", metric="haversine", k = 2, max_temporal_distance=180):
    '''
    Finds first and second nearest neighbors for a buoy dataframe to find 
    (i) first and second nearest NOOA/DFO stations and (ii) first and second
    nearest buoys

    Inputs(s):
        - fname_buoys (str): file path name for buoy data (tsv format)
        - fname_stations (str): file path name for stations data (tsv format)
        - metric (str or DistanceMetric object): Distance metric to use for the tree.
            For a two-dimensional vector space (lat/lon), a "haversine" distance 
            metric is assumed
    Output(s):
        - buoys (GeoDataFrame): Contains rows corresponding to unique buoy, 
            timestamp data with corresponding nearest station and distinct 
            buoy neighbors
    '''
    buoys, stations = load_data(fname_buoys, fname_stations)

    # Find first and second nearest buoy neighbors
    buoys = find_nearest_buoys(buoys, metric, k, max_temporal_distance)

    # Find first and second nearest station neighbors
    first_closest_stations, second_closest_stations = get_nearest(buoys, stations, metric)

    # Rename station columns and join station neighbors dfs and original buoy df
    first_closest_stations.columns = ['fst_' + str(col) for col in first_closest_stations.columns]
    second_closest_stations.columns = ['sst_' + str(col) for col in second_closest_stations.columns]
    buoys = buoys.join(first_closest_stations).join(second_closest_stations)

    # Save file
    filename = re.search('buoys/(.*).tsv', str(fname_buoys)).group(1) + "_stations.tsv"
    files.save_df(buoys, filename, index=False)

    return buoys

if __name__ == "__main__":
    go("buoys/buoys_228_229_230.tsv", max_temporal_distance=180)
