import numpy as np

long_short_name_dict = {
    "2m_temperature": "t2m",
}

lat_range = np.arange(-90, 90.1, 0.25)
lon_range = np.arange(-180, 180.1, 0.25)


def time_resolution_to_freq(time_resolution):
    if time_resolution == "hour":
        return "h"
    elif time_resolution == "day":
        return "D"
    elif time_resolution == "month":
        return "ME"
    elif time_resolution == "year":
        return "YE"
    else:
        raise ValueError("Invalid time_resolution")
