from abc import ABC, abstractmethod
import xarray as xr

from .metadata import Metadata
from .utils.const import long_short_name_dict


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
        metadata=None,  # metadata file path
    ):
        if temporal_resolution == "hour":
            temporal_aggregation = None
        if spatial_resolution == 0.25:
            spatial_aggregation = None
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
        if metadata:
            self.metadata = Metadata(metadata)
        else:
            self.metadata = Metadata("metadata.csv")

    @abstractmethod
    def execute(self) -> xr.Dataset:
        """
        Return: xarray.Dataset, with data variable as lazy-loaded Dask array (if applicable)
        """
        pass

    def compute(self) -> xr.Dataset:
        """
        Return: xarray.Dataset, with data variable as loaded-in-memory Numpy array
        """
        return self.execute().compute()
