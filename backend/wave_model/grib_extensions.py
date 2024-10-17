import json
import numpy as np
import cfgrib
import psycopg2
from scipy.spatial import cKDTree
import os


class WaveModel:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="stations",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        self.cursor = self.conn.cursor()

    # Load surf spots from JSON
    def load_surf_spots(self, json_file):
        """Loads the surf spots JSON file."""
        with open(json_file, "r") as file:
            surf_spots = json.load(file)
        return surf_spots

    # Extract GRIB metadata using cfgrib
    def extract_grib_metadata(self, grib_data):
        """Extracts and returns relevant metadata from GRIB data."""
        metadata = {}
        metadata["units"] = grib_data.units
        metadata["data_date"] = grib_data.time.dt.strftime("%Y%m%d").item()  # Date
        metadata["data_time"] = grib_data.time.dt.strftime("%H%M").item()  # Time
        metadata["forecast_time"] = (
            grib_data.step_hours.item()
        )  # Forecast time in hours
        return metadata

    # Extract and filter GRIB data
    def extract_and_filter_data(self, grib_data):
        """Extracts the data array, lats, and lons from GRIB, and filters missing values."""
        values = grib_data.values
        lats = grib_data.latitude.values
        lons = grib_data.longitude.values

        # Handle missing data
        missing_value_indicator = grib_data.missing_value
        filtered_values = np.where(values == missing_value_indicator, np.nan, values)

        return filtered_values, lats, lons

    # Insert data into PostgreSQL
    def insert_surf_data(self, conn, spot, param_name, value, metadata):
        """Inserts extracted surf data into a PostgreSQL database."""
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

    # Find the nearest point for a given lat/lon in the GRIB data
    def find_nearest_point(self, latitudes, longitudes, surf_spot_lat, surf_spot_lng):
        """Finds the nearest point in the GRIB data for the given latitude and longitude."""
        lat_lon_pairs = np.column_stack([latitudes.ravel(), longitudes.ravel()])
        tree = cKDTree(lat_lon_pairs)
        _, idx = tree.query([surf_spot_lat, surf_spot_lng])
        return idx

    # Process the GRIB file
    def process_grib_file(self, grib_file, surf_spots, conn):
        """Processes the GRIB file for each surf spot and inserts data into the PostgreSQL database."""
        # Load the GRIB file using cfgrib
        grib_data = cfgrib.open_datasets(grib_file)

        # Assume only one dataset (multiple variables could be handled differently)
        for dataset in grib_data:
            # For each variable (wind speed, wave height, etc.)
            for var in dataset.variables:
                # Get the data and metadata for this variable
                data_array = dataset[var]
                metadata = self.extract_grib_metadata(data_array)

                # Extract and filter data
                values, latitudes, longitudes = self.extract_and_filter_data(data_array)

                # Loop over surf spots to extract the nearest value
                for spot in surf_spots:
                    idx = self.find_nearest_point(
                        latitudes, longitudes, float(spot["lat"]), float(spot["lng"])
                    )
                    nearest_value = values.ravel()[idx]

                    # Insert the surf spot and GRIB data into the database
                    self.insert_surf_data(conn, spot, var, nearest_value, metadata)


# Main function to load JSON, process GRIB, and insert into PostgreSQL
def main():
    wave_model = WaveModel()
    # Load surf spots
    json_file = "data/surfspots.json"
    surf_spots = wave_model.load_surf_spots(json_file)

    try:
        # Process the GRIB file for all surf spots
        grib_file = os.path.expanduser("~/Downloads/gfswave.t06z.arctic.9km.f000.grib2")
        wave_model.process_grib_file(grib_file, surf_spots)

    finally:
        wave_model.close()


if __name__ == "__main__":
    main()
