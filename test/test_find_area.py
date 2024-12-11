import unittest
import xarray as xr

import sys
import os

# Add the 'src' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from query_executor_find_area import FindAreaExecutor

variable = "2m_temperature"
# Greenland
max_lat = 85
min_lat = 60
min_lon = -70
max_lon = -10


class TestFindArea(unittest.TestCase):

    def _test_suite(self, start_dt, end_dt, agg, filter_pred, filter_val):
        qe = FindAreaExecutor(
            variable=variable,
            start_datetime=start_dt,
            end_datetime=end_dt,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            heatmap_aggregation_method=agg,
            filter_predicate=filter_pred,
            filter_value=filter_val,
        )
        res = qe.execute()
        self.assertIsInstance(res, xr.Dataset)
        self.assertEqual(len(res.dims), 2)
        self.assertGreater(res.latitude.size, 0)
        self.assertGreater(res.longitude.size, 0)

    def test_whole_year_mean(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        heatmap_aggregation_method = "mean"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, heatmap_aggregation_method, filter_predicate, filter_value)

    def test_whole_year_max(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        heatmap_aggregation_method = "max"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, heatmap_aggregation_method, filter_predicate, filter_value)

    def test_whole_year_min(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        heatmap_aggregation_method = "min"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, heatmap_aggregation_method, filter_predicate, filter_value)

    def test_year_month_day_hour_mean(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        heatmap_aggregation_method = "mean"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, heatmap_aggregation_method, filter_predicate, filter_value)

    def test_year_month_day_hour_max(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        heatmap_aggregation_method = "max"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, heatmap_aggregation_method, filter_predicate, filter_value)

    def test_year_month_day_hour_min(self):
        start_datetime = "2020-01-01 10:00:00"
        end_datetime = "2023-12-31 20:00:00"
        heatmap_aggregation_method = "min"
        filter_predicate = ">"
        filter_value = 263
        self._test_suite(start_datetime, end_datetime, heatmap_aggregation_method, filter_predicate, filter_value)
