import eccodes as ec
import numpy as np
import matplotlib.pyplot as plt

# Path to your GRIB file
grib_file = (
    "/usr/local/google/home/lacectk/Downloads/gfswave.t18z.arctic.9km.f000.grib2"
)


def extract_grib_metadata(gid):
    """
    Extracts and prints relevant metadata from the GRIB message.
    """
    # Basic metadata
    param_name = ec.codes_get(gid, "shortName")
    units = ec.codes_get(gid, "units")
    data_date = ec.codes_get(gid, "dataDate")
    data_time = ec.codes_get(gid, "dataTime")
    forecast_time = ec.codes_get(gid, "forecastTime")
    level = ec.codes_get(gid, "level")
    grid_type = ec.codes_get(gid, "gridType")
    lat_count = ec.codes_get(gid, "Ni")
    lon_count = ec.codes_get(gid, "Nj")

    print(f"Parameter: {param_name}")
    print(f"Units: {units}")
    print(f"Date: {data_date}, Time: {data_time}, Forecast Time: {forecast_time} hours")
    print(f"Level: {level}")
    print(f"Grid Type: {grid_type}, Dimensions: {lat_count} x {lon_count}")


def extract_and_filter_data(gid):
    """
    Extracts values from the GRIB message and filters out missing data.
    """
    values = ec.codes_get_values(gid)

    # Filter out missing data (9999 is often used as missing data in GRIB files)
    missing_value_indicator = ec.codes_get(gid, "missingValue")
    filtered_values = np.where(values == missing_value_indicator, np.nan, values)

    return filtered_values


def main():
    with open(grib_file, "rb") as f:
        # Iterate through the GRIB messages
        gid = ec.codes_grib_new_from_file(f)
        if gid is None:
            print("Failed to open GRIB message.")
            return

        # Extract metadata and display it
        extract_grib_metadata(gid)

        # Extract and filter data
        values = extract_and_filter_data(gid)
        print(f"Extracted {len(values)} values (after filtering missing data).")
        print("First 10 data points (after filtering):", values[:10])

        # Release the GRIB message
        ec.codes_release(gid)


if __name__ == "__main__":
    main()
