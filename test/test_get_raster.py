import unittest
import xarray as xr

import sys
import os

# Add the 'src' directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from query_executor_get_raster import GetRasterExecutor

variable = "2m_temperature"
# Greenland
max_lat = 85
min_lat = 60
min_lon = -70
max_lon = -10


class TestGetRaster(unittest.TestCase):

    def _test_suite(self, start_dt, end_dt, time_res, time_agg):
        qe = GetRasterExecutor(
            variable=variable,
            start_datetime=start_dt,
            end_datetime=end_dt,
            temporal_resolution=time_res,
            temporal_aggregation=time_agg,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        )
        res = qe.execute()
        self.assertIsInstance(res, xr.Dataset)
        self.assertEqual(len(res.dims), 3)
        self.assertGreater(res.time.size, 0)
        self.assertGreater(res.latitude.size, 0)
        self.assertGreater(res.longitude.size, 0)

    def test_year(self):
        start_datetime = "2020-01-01 00:00:00"
        end_datetime = "2023-12-31 23:00:00"
        temporal_resolution = "year"
        temporal_aggregation = "mean"
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_hour(self):
        start_datetime = "2020-05-10 10:00:00"
        end_datetime = "2022-10-10 20:00:00"
        temporal_resolution = "hour"
        temporal_aggregation = None
        self._test_suite(start_datetime, end_datetime, temporal_resolution, temporal_aggregation)

    def test_api_hour(self):
        max_lat = 85
        min_lat = 84
        min_lon = -11
        max_lon = -10

        start_datetime = "2023-10-10 10:00:00"
        end_datetime = "2024-02-10 20:00:00"
        temporal_resolution = "hour"
        temporal_aggregation = None
        qe = GetRasterExecutor(
            variable=variable,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            temporal_resolution=temporal_resolution,
            temporal_aggregation=temporal_aggregation,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        )
        res = qe.execute()
        self.assertIsInstance(res, xr.Dataset)
        self.assertEqual(len(res.dims), 3)
        self.assertGreater(res.time.size, 0)
        self.assertGreater(res.latitude.size, 0)
        self.assertGreater(res.longitude.size, 0)
