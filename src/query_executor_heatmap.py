import xarray as xr

from query_executor import QueryExecutor
from query_executor_get_raster import GetRasterExecutor
from utils.get_whole_period import get_whole_ranges_between


class HeatmapExecutor(QueryExecutor):
    def __init__(
        self,
        variable: str,
        start_datetime: str,
        end_datetime: str,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        heatmap_aggregation_method: str,  # e.g., "mean", "max", "min"
        spatial_resolution=0.25,  # e.g., 0.25, 0.5, 1.0
        spatial_aggregation=None,  # e.g., "mean", "max", "min"
    ):
        super().__init__(
            variable,
            start_datetime,
            end_datetime,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            spatial_resolution=spatial_resolution,
            spatial_aggregation=spatial_aggregation,
        )
        self.heatmap_aggregation_method = heatmap_aggregation_method

    def execute(self):
        if self.heatmap_aggregation_method == "mean":
            return self._get_mean_heatmap()
        elif self.heatmap_aggregation_method == "max":
            return self._get_max_heatmap()
        elif self.heatmap_aggregation_method == "min":
            return self._get_min_heatmap()
        else:
            raise ValueError("Invalid heatmap_aggregation_method")

    def _get_mean_heatmap(self):
        return None

    def _get_max_heatmap(self):
        return None

    def _get_min_heatmap(self):
        year_range, month_range, day_range, hour_range = get_whole_ranges_between(
            self.start_datetime, self.end_datetime
        )
        ds_year = []
        ds_month = []
        ds_day = []
        ds_hour = []
        for start_year, end_year in year_range:
            get_raster_year = GetRasterExecutor(
                self.variable,
                start_year,
                end_year,
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="year",
                temporal_aggregation=self.heatmap_aggregation_method,
            )
            ds_year.append(get_raster_year.execute())
        for start_month, end_month in month_range:
            get_raster_month = GetRasterExecutor(
                self.variable,
                start_month,
                end_month,
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="month",
                temporal_aggregation=self.heatmap_aggregation_method,
            )
            ds_month.append(get_raster_month.execute())
        for start_day, end_day in day_range:
            get_raster_day = GetRasterExecutor(
                self.variable,
                start_day,
                end_day,
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="day",
                temporal_aggregation=self.heatmap_aggregation_method,
            )
            ds_day.append(get_raster_day.execute())
        for start_hour, end_hour in hour_range:
            get_raster_hour = GetRasterExecutor(
                self.variable,
                start_hour,
                end_hour,
                self.min_lat,
                self.max_lat,
                self.min_lon,
                self.max_lon,
                temporal_resolution="hour",
                temporal_aggregation=self.heatmap_aggregation_method,
            )
            ds_hour.append(get_raster_hour.execute())

        # get min heatmap from ds_year, ds_month, ds_day, ds_hour
        ds_min = []
        for ds in [ds_year, ds_month, ds_day, ds_hour]:
            if ds:
                ds_min.append(xr.concat(ds, dim="time").min(dim="time"))
        heatmap = xr.concat(ds_min, dim="time").min(dim="time")
        return heatmap
