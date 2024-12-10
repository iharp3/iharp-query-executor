import unittest
import xarray as xr

import sys
import os

# Add the 'src' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from query_executor_find_time import FindTimeExecutor

variable = "2m_temperature"
# Greenland
max_lat = 85
min_lat = 60
min_lon = -70
max_lon = -10


class TestFindTime(unittest.TestCase):

    def _test_suite(self, start_dt, end_dt, time_res, time_agg, filter_pred, filter_val):
        qe = FindTimeExecutor(
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
            filter_predicate=filter_pred,
            filter_value=filter_val,
        )
        res = qe.execute()
        self.assertIsInstance(res, xr.Dataset)
        self.assertEqual(len(res.dims), 1)
        self.assertGreater(res.time.size, 0)

    def test_yearly(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        time_resolution = "year"
        time_agg_method = "mean"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, time_resolution, time_agg_method, filter_predicate, filter_value)

    def test_hourly(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        time_resolution = "hour"
        time_agg_method = "mean"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, time_resolution, time_agg_method, filter_predicate, filter_value)

    def test_hourly_2(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        time_resolution = "hour"
        time_agg_method = "mean"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, time_resolution, time_agg_method, filter_predicate, filter_value)

    def test_hourly_3(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        time_resolution = "hour"
        time_agg_method = "mean"
        filter_predicate = ">"
        filter_value = 363
        self._test_suite(start_datetime, end_datetime, time_resolution, time_agg_method, filter_predicate, filter_value)
