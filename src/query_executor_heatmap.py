from query_executor import QueryExecutor
from utils.get_whole_period import get_whole_period_between


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
        return None
