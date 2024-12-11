from abc import ABC, abstractmethod

from metadata import Metadata
from utils.const import long_short_name_dict


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
        self.metadata = Metadata("/data/iharp-customized-storage/storage/metadata.csv")  # needs to be changed
        # customize to Ana's metadata
        # add columns
        self.metadata.df_meta["file_path"] = (
            "/data/iharp-customized-storage/storage/" + self.metadata.df_meta["file_name"]
        )
        # rename columns
        self.metadata.df_meta = self.metadata.df_meta.rename(
            columns={
                "max_lat_N": "max_lat",
                "min_lat_S": "min_lat",
                "max_long_E": "max_lon",
                "min_long_W": "min_lon",
                "start_time": "start_datetime",
                "end_time": "end_datetime",
                "temporal_agg_type": "temporal_aggregation",
                "spatial_agg_type": "spatial_aggregation",
            }
        )
        # change literal conversion
        self.metadata.df_meta["temporal_resolution"] = self.metadata.df_meta["temporal_resolution"].apply(
            lambda x: {"H": "hour", "D": "day", "M": "month", "Y": "year"}[x]
        )

    @abstractmethod
    def execute(self):
        pass
