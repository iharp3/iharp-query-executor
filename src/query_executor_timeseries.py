from .query_executor import *
from .query_executor_get_raster import GetRasterExecutor


class TimeseriesExecutor(QueryExecutor):
    def __init__(
        self,
        variable: str,
        start_datetime: str,
        end_datetime: str,
        temporal_resolution: str,  # e.g., "hour", "day", "month", "year"
        temporal_aggregation: str,  # e.g., "mean", "max", "min"
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        time_series_aggregation_method: str,  # e.g., "mean", "max", "min"
        spatial_resolution=1.0,  # e.g., 0.25, 0.5, 1.0
        spatial_aggregation="mean",  # e.g., "mean", "max", "min"
        metadata=None,  # metadata file path
    ):
        super().__init__(
            variable,
            start_datetime,
            end_datetime,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            temporal_resolution,
            temporal_aggregation,
            spatial_resolution=spatial_resolution,
            spatial_aggregation=spatial_aggregation,
            metadata=metadata,
        )
        self.time_series_aggregation_method = time_series_aggregation_method

    def execute(self):
        get_raster_executor = GetRasterExecutor(
            metadata=self.metadata.f_path,
            variable=self.variable,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            min_lat=self.min_lat,
            max_lat=self.max_lat,
            min_lon=self.min_lon,
            max_lon=self.max_lon,
            temporal_resolution=self.temporal_resolution,
            temporal_aggregation=self.temporal_aggregation,
            spatial_resolution=self.spatial_resolution,
            spatial_aggregation=self.spatial_aggregation,
        )
        raster = get_raster_executor.execute()
        if self.time_series_aggregation_method == "mean":
            return raster.mean(dim=["latitude", "longitude"])
        elif self.time_series_aggregation_method == "max":
            return raster.max(dim=["latitude", "longitude"])
        elif self.time_series_aggregation_method == "min":
            return raster.min(dim=["latitude", "longitude"])
        else:
            raise ValueError(f"Invalid time series aggregation method: {self.time_series_aggregation_method}")
