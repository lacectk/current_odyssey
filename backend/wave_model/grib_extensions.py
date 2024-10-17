import json
import eccodes as ec
import numpy as np
import os
import psycopg2
from scipy.spatial import cKDTree


# Load surf spots from JSON
def load_surf_spots(json_file):
    with open(json_file, "r") as file:
        surf_spots = json.load(file)
    return surf_spots


def inspect_grib_file(grib_file):
    with open(grib_file, "rb") as f:
        while True:
            gid = ec.codes_grib_new_from_file(f)
            if gid is None:
                break

            # Get and print all key information
            keys = ec.codes_keys_iterator_new(gid)
            while ec.codes_keys_iterator_next(keys):
                key_name = ec.codes_keys_iterator_get_name(keys)
                try:
                    value = ec.codes_get(gid, key_name)
                    print(f"{key_name}: {value}")
                except Exception:
                    continue
            ec.codes_release(gid)


# Extract metadata from GRIB
def extract_grib_metadata(gid):
    metadata = {}
    metadata["units"] = ec.codes_get(gid, "units")
    metadata["data_date"] = ec.codes_get(gid, "dataDate")
    metadata["data_time"] = ec.codes_get(gid, "dataTime")
    metadata["forecast_time"] = ec.codes_get(gid, "forecastTime")
    return metadata


# Extract data for each parameter
def extract_and_filter_data(gid):
    values = ec.codes_get_values(gid)
    latitudes = ec.codes_get_array(gid, "distinctLatitudes")
    longitudes = ec.codes_get_array(gid, "distinctLongitudes")

    # Filter missing data
    missing_value_indicator = ec.codes_get(gid, "missingValue")
    filtered_values = np.where(values == missing_value_indicator, np.nan, values)

    return filtered_values, latitudes, longitudes


# Insert data into PostgreSQL
def insert_surf_data(conn, spot, param_name, value, metadata):
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO surf_spot_data (surf_spot_name, country, lat, lng, 
    parameter_name, value, units, data_date, data_time, forecast_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(
        insert_query,
        (
            spot["name"],
            spot["country"],
            spot["lat"],
            spot["lng"],
            param_name,
            value,
            metadata["units"],
            metadata["data_date"],
            metadata["data_time"],
            metadata["forecast_time"],
        ),
    )

    conn.commit()
    cursor.close()


# Get the nearest grid point from GRIB file to the surf spot
def find_nearest_point(latitudes, longitudes, surf_spot_lat, surf_spot_lng):
    lat_lon_pairs = np.column_stack([latitudes, longitudes])
    tree = cKDTree(lat_lon_pairs)
    _, idx = tree.query([surf_spot_lat, surf_spot_lng])
    return idx


def process_grib_file(grib_file, surf_spots, conn):
    with open(grib_file, "rb") as f:
        while True:
            gid = ec.codes_grib_new_from_file(f)
            if gid is None:
                break

            # Extract metadata
            metadata = extract_grib_metadata(gid)

            # Get parameter name
            param_name = ec.codes_get(gid, "name")

            # Extract values, latitudes, and longitudes
            values, latitudes, longitudes = extract_and_filter_data(gid)

            # Loop over surf spots to extract the nearest value
            for spot in surf_spots:
                idx = find_nearest_point(
                    latitudes, longitudes, float(spot["lat"]), float(spot["lng"])
                )
                nearest_value = values[idx]

                # Insert the surf spot and GRIB data into the database
                insert_surf_data(conn, spot, param_name, nearest_value, metadata)

            ec.codes_release(gid)


def main():
    # json_file = "data/surfspots.json"
    # surf_spots = load_surf_spots(json_file)

    # # Connect to PostgreSQL
    # conn = psycopg2.connect(
    #     dbname="stations",
    #     user=os.getenv("DB_USER"),
    #     password=os.getenv("DB_PASSWORD"),
    #     host=os.getenv("DB_HOST"),
    # )

    # try:
    #     # Process the GRIB file for all surf spots
    #     grib_file = os.path.expanduser("~/Downloads/gfswave.t06z.arctic.9km.f000.grib2")
    #     process_grib_file(grib_file, surf_spots, conn)

    # finally:
    #     conn.close()
    grib_file = os.path.expanduser("~/Downloads/gfswave.t06z.arctic.9km.f000.grib2")
    inspect_grib_file(grib_file)



if __name__ == "__main__":
    main()
