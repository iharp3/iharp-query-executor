from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime

lat_range = np.arange(-90, 90.1, 0.25)
lon_range = np.arange(-180, 180.1, 0.25)
long_short_name_dict = {
    "2m_temperature": "t2m",
}


def time_resolution_to_freq(time_resolution):
    if time_resolution == "hour":
        return "h"
    elif time_resolution == "day":
        return "D"
    elif time_resolution == "month":
        return "ME"
    elif time_resolution == "year":
        return "YE"
    else:
        raise ValueError("Invalid time_resolution")


class QueryExecutor(ABC):
    def __init__(
        self,
        variable: str,
        start_datetime: str,
        end_datetime: str,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        temporal_resolution="hour",  # e.g., "hour", "day", "month", "year"
        temporal_aggregation=None,  # e.g., "mean", "max", "min"
        spatial_resolution=0.25,  # e.g., 0.25, 0.5, 1.0
        spatial_aggregation=None,  # e.g., "mean", "max", "min"
    ):
        # user query parameters
        self.variable = variable
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.temporal_resolution = temporal_resolution
        self.temporal_aggregation = temporal_aggregation
        self.spatial_resolution = spatial_resolution
        self.spatial_aggregation = spatial_aggregation

        # query internal variables
        self.variable_short_name = long_short_name_dict[self.variable]
        self.df_meta = pd.read_csv("metadata.csv")  # needs to be changed

    @abstractmethod
    def execute(self):
        pass

    def get_relevant_meta(self):
        df_relevant = self.df_meta[
            (self.df_meta["temporal_resolution"] == self.temporal_resolution)
            & (self.df_meta["temporal_aggregation"] == self.temporal_aggregation)
            & (self.df_meta["spatial_resolution"] == self.spatial_resolution)
            & (self.df_meta["spatial_aggregation"] == self.spatial_aggregation)
            & (self.df_meta["min_lat"] <= self.max_lat)
            & (self.df_meta["max_lat"] >= self.min_lat)
            & (self.df_meta["min_lon"] <= self.max_lon)
            & (self.df_meta["max_lon"] >= self.min_lon)
            & (pd.to_datetime(self.df_meta["start_datetime"]) <= pd.to_datetime(self.end_datetime))
            & (pd.to_datetime(self.df_meta["end_datetime"]) >= pd.to_datetime(self.start_datetime))
        ]
        return df_relevant

    def gen_empty_query_result_xarray(self):
        lat_start = lat_range.searchsorted(self.min_lat, side="left")
        lat_end = lat_range.searchsorted(self.max_lat, side="right")
        lon_start = lon_range.searchsorted(self.min_lon, side="left")
        lon_end = lon_range.searchsorted(self.max_lon, side="right")
        ds_empty = xr.Dataset()
        ds_empty["latitude"] = lat_range[lat_start:lat_end]
        ds_empty["longitude"] = lon_range[lon_start:lon_end]
        ds_empty["time"] = pd.date_range(
            start=self.start_datetime,
            end=self.end_datetime,
            freq=time_resolution_to_freq(self.temporal_resolution),
        )
        return ds_empty

    @staticmethod
    def gen_xarray_for_meta(row):
        lat_start = lat_range.searchsorted(row["min_lat"], side="left")
        lat_end = lat_range.searchsorted(row["max_lat"], side="right")
        lon_start = lon_range.searchsorted(row["min_lon"], side="left")
        lon_end = lon_range.searchsorted(row["max_lon"], side="right")
        ds = xr.Dataset()
        ds["latitude"] = lat_range[lat_start:lat_end]
        ds["longitude"] = lon_range[lon_start:lon_end]
        ds["time"] = pd.date_range(
            start=row["start_datetime"],
            end=row["end_datetime"],
            freq=time_resolution_to_freq(row["resolution"]),
        )
        return ds

    @staticmethod
    def mask_query_with_meta(ds_query, ds_meta):
        return (
            ds_query["latitude"].isin(ds_meta["latitude"])
            & ds_query["longitude"].isin(ds_meta["longitude"])
            & ds_query["time"].isin(ds_meta["time"])
        )

    @staticmethod
    def gen_download_file_name():
        datatime = datetime.now().strftime("%Y%m%d_%H-%M-%S")
        return f"download_{datatime}.nc"
