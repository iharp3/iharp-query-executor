import numpy as np
import xarray as xr

from query_executor import QueryExecutor
from query_executor_get_raster import GetRasterExecutor
from utils.get_whole_period import (
    get_whole_ranges_between,
    get_total_hours_in_year,
    get_total_hours_in_month,
    iterate_months,
    number_of_days_inclusive,
    number_of_hours_inclusive,
    get_total_hours_between,
)


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
        year_range, month_range, day_range, hour_range = get_whole_ranges_between(
            self.start_datetime, self.end_datetime
        )
        ds_year = []
        ds_month = []
        ds_day = []
        ds_hour = []
        year_hours = []
        month_hours = []
        day_hours = []
        hour_hours = []
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
                temporal_aggregation="mean",
            )
            ds_year.append(get_raster_year.execute())
            year_hours += [get_total_hours_in_year(y) for y in range(start_year, end_year + 1)]
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
                temporal_aggregation="mean",
            )
            ds_month.append(get_raster_month.execute())
            month_hours += [get_total_hours_in_month(m) for m in iterate_months(start_month, end_month)]
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
                temporal_aggregation="mean",
            )
            ds_day.append(get_raster_day.execute())
            day_hours += [24 for _ in range(number_of_days_inclusive(start_day, end_day))]
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
                temporal_aggregation="mean",
            )
            ds_hour.append(get_raster_hour.execute())
            hour_hours += [1 for _ in range(number_of_hours_inclusive(start_hour, end_hour))]

        xrds_concat = xr.concat(ds_year + ds_month + ds_day + ds_hour, dim="time")
        nd_array = xrds_concat[self.variable_short_name].to_numpy()
        weights = np.array(year_hours + month_hours + day_hours + hour_hours)
        total_hours = get_total_hours_between(self.start_datetime, self.end_datetime)
        weights = weights / total_hours
        average = np.average(nd_array, axis=0, weights=weights)
        res = xr.Dataset(
            {self.variable_short_name: (["latitude", "longitude"], average)},
            coords={"latitude": xrds_concat.latitude, "longitude": xrds_concat.longitude},
        )
        return res

    def _get_max_heatmap(self):
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
                temporal_aggregation="max",
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
                temporal_aggregation="max",
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
                temporal_aggregation="max",
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
                temporal_aggregation="max",
            )
            ds_hour.append(get_raster_hour.execute())

        return xr.concat(ds_year + ds_month + ds_day + ds_hour, dim="time").max(dim="time")

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
        return xr.concat(ds_year + ds_month + ds_day + ds_hour, dim="time").min(dim="time")
