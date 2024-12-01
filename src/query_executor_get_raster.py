import cdsapi
import xarray as xr

from query_executor import *


class GetRasterExecutor(QueryExecutor):
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
            spatial_resolution,
            spatial_aggregation,
        )

    def _check_metadata(self):
        """
        Return: [local_files], [api_calls]
        """
        df_overlap, leftover = self.metadata.query_get_overlap_and_residual(
            self.variable,
            self.start_datetime,
            self.end_datetime,
            self.min_lat,
            self.max_lat,
            self.min_lon,
            self.max_lon,
            self.temporal_resolution,
            self.temporal_aggregation,
            self.spatial_resolution,
            self.spatial_aggregation
        )

        local_files = df_overlap["file_path"].tolist()
        api_calls = []
        if leftover is not None:
            leftover_min_lat = leftover.latitude.min().item()
            leftover_max_lat = leftover.latitude.max().item()
            leftover_min_lon = leftover.longitude.min().item()
            leftover_max_lon = leftover.longitude.max().item()
            leftover_start_datetime = str(leftover.time.min().values)
            leftover_end_datetime = str(leftover.time.max().values)

            dataset = "reanalysis-era5-single-levels"
            request = {
                "product_type": ["reanalysis"],
                "variable": [self.variable],
                "year": [range(int(leftover_start_datetime[:4]), int(leftover_end_datetime[:4]) + 1)],
                "month": [str(i).zfill(2) for i in range(1, 13)],
                "day": [str(i).zfill(2) for i in range(1, 32)],
                "time": [f"{str(i).zfill(2)}:00" for i in range(0, 24)],
                "data_format": "netcdf",
                "download_format": "unarchived",
                "area": [leftover_max_lat, leftover_min_lon, leftover_min_lat, leftover_max_lon],
            }
            api_calls.append((dataset, request))
        return local_files, api_calls

    def execute(self):
        file_list, api = self._check_metadata()

        # 1. call apis
        if api:
            c = cdsapi.Client()
            for dataset, request in api:
                file_name = gen_download_file_name()
                c.retrieve(dataset, request).download(file_name)
                file_list.append(file_name)

        # 2. execute query
        ds_list = []
        for file in file_list:
            ds = xr.open_dataset(file, engine="netcdf4").sel(
                time=slice(self.start_datetime, self.end_datetime),
                latitude=slice(self.max_lat, self.min_lat),
                longitude=slice(self.min_lon, self.max_lon),
            )
            ds_list.append(ds)
        ds = xr.concat([i.chunk() for i in ds_list], dim="time")
        return ds
