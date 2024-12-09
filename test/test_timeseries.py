import unittest
import xarray as xr

import sys
import os

# Add the 'src' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from query_executor_timeseries import TimeseriesExecutor

variable = "2m_temperature"
# Greenland
max_lat = 85
min_lat = 60
min_lon = -70
max_lon = -10


class TestTimeseries(unittest.TestCase):

    def _test_suite(self, start_dt, end_dt, time_res, time_agg):
        qe = TimeseriesExecutor(
            variable=variable,
            start_datetime=start_dt,
            end_datetime=end_dt,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            temporal_resolution=time_res,
            temporal_aggregation=time_agg,
            time_series_aggregation_method=time_agg,
        )
        res = qe.execute()
        self.assertIsInstance(res, xr.Dataset)
        self.assertEqual(len(res.dims), 1)
        self.assertGreater(res.time.size, 0)

    def test_whole_year_mean(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        temporal_resolution = "year"
        temporal_aggregation = "mean"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_whole_year_max(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        temporal_resolution = "year"
        temporal_aggregation = "max"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_whole_year_min(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        temporal_resolution = "year"
        temporal_aggregation = "min"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_year_month_day_hour_mean(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        temporal_resolution = "hour"
        temporal_aggregation = "mean"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_year_month_day_hour_max(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        temporal_resolution = "hour"
        temporal_aggregation = "max"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_year_month_day_hour_min(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        temporal_resolution = "hour"
        temporal_aggregation = "min"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)
