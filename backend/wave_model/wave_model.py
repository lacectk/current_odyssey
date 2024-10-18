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

    def setup_database(self):
        try:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS swell (
                    id SERIAL PRIMARY KEY,
                    surf_spot_name VARCHAR(255),
                    country VARCHAR(255),
                    lat FLOAT,
                    lng FLOAT,
                    wind_speed FLOAT,
                    wind_direction FLOAT,
                    u_component_of_wind FLOAT,
                    v_component_of_wind FLOAT,
                    sig_height_combined_waves FLOAT,
                    primary_wave_mean_period FLOAT,
                    primary_wave_direction FLOAT,
                    sig_height_wind_waves FLOAT,
                    sig_height_total_swell FLOAT,
                    mean_period_wind_waves FLOAT,
                    mean_period_total_swell FLOAT,
                    direction_wind_waves FLOAT,
                    direction_swell_waves FLOAT,
                    units VARCHAR(50),
                    date DATE,
                    time TIME,
                    forecast_time INTERVAL
                );
                """
            )
            self.conn.commit()
            print("Table 'swell' checked/created successfully.")

        except Exception as e:
            print(f"Error setting up the database: {e}")
            self.conn.rollback()

    # Insert data into PostgreSQL
    def insert_surf_data(self, spot, params, metadata):
        """Inserts extracted surf data into a PostgreSQL database."""

        insert_query = """
        INSERT INTO swell (surf_spot_name, country, lat, lng,
            wind_speed, wind_direction, u_component_of_wind,
            v_component_of_wind, sig_height_combined_waves,
            primary_wave_mean_period, primary_wave_direction,
            sig_height_wind_waves, sig_height_total_swell,
            mean_period_wind_waves, mean_period_total_swell,
            direction_wind_waves, direction_swell_waves,
            units, date, time, forecast_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        '%s, %s, %s, %s, %s)
        """

        self.cursor.execute(
            insert_query,
            (
                spot["name"],
                spot["country"],
                spot["lat"],
                spot["lng"],
                params.get("wind_speed"),
                params.get("wind_direction"),
                params.get("u_component_of_wind"),
                params.get("v_component_of_wind"),
                params.get("sig_height_combined_waves"),
                params.get("primary_wave_mean_period"),
                params.get("primary_wave_direction"),
                params.get("sig_height_wind_waves"),
                params.get("sig_height_total_swell"),
                params.get("mean_period_wind_waves"),
                params.get("mean_period_total_swell"),
                params.get("direction_wind_waves"),
                params.get("direction_swell_waves"),
                metadata["units"],
                metadata["data_date"],
                metadata["data_time"],
                metadata["forecast_time"],
            ),
        )

        self.conn.commit()

    # Find the nearest point for a given lat/lon in the GRIB data
    def find_nearest_point(self, latitudes, longitudes, surf_spot_lat, surf_spot_lng):
        """Finds the nearest point in the GRIB data for the given latitude and longitude."""
        lat_lon_pairs = np.column_stack([latitudes.ravel(), longitudes.ravel()])
        tree = cKDTree(lat_lon_pairs)
        _, idx = tree.query([surf_spot_lat, surf_spot_lng])
        return idx

    # Process the GRIB file
    def process_grib_file(self, grib_file, surf_spots):
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

    async def close(self):
        self.cursor.close()
        self.conn.close()


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
