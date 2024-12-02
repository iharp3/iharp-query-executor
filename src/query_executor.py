from abc import ABC, abstractmethod
from datetime import datetime

from metadata import Metadata
from utils.const import long_short_name_dict


def gen_download_file_name():
    dt = datetime.now().strftime("%Y%m%d_%H-%M-%S")
    return f"download_{dt}.nc"


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
        self.metadata = Metadata("metadata.csv")  # needs to be changed

    @abstractmethod
    def execute(self):
        pass
