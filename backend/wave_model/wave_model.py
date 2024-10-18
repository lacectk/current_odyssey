import json
import numpy as np
import pygrib
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

    # Extract GRIB metadata using pygrib
    def extract_grib_metadata(self, grb):
        """Extracts and returns relevant metadata from the GRIB message."""
        metadata = {
            "units": grb.units,
            "data_date": grb.analDate.strftime("%Y%m%d"),
            "data_time": grb.analDate.strftime("%H%M"),
            "forecast_time": grb.forecastTime,
        }
        return metadata

    # Extract and filter GRIB data
    def extract_and_filter_data(self, grb):
        """Extracts the data array, lats, and lons from GRIB, and filters missing values."""
        values = grb.values
        lats, lons = grb.latlons()

        # Handle missing data: PyGRIB uses 9999.0 as a missing value by default
        missing_value_indicator = 9999.0
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
                    wind_speed_units VARCHAR(50),
                    wind_direction FLOAT,
                    wind_direction_units VARCHAR(50),
                    u_component_of_wind FLOAT,
                    u_component_of_wind_units VARCHAR(50),
                    v_component_of_wind FLOAT,
                    v_component_of_wind_units VARCHAR(50),
                    sig_height_combined_waves FLOAT,
                    sig_height_combined_waves_units VARCHAR(50),
                    primary_wave_mean_period FLOAT,
                    primary_wave_mean_period_units VARCHAR(50),
                    primary_wave_direction FLOAT,
                    primary_wave_direction_units VARCHAR(50),
                    sig_height_wind_waves FLOAT,
                    sig_height_wind_waves_units VARCHAR(50),
                    sig_height_total_swell FLOAT,
                    sig_height_total_swell_units VARCHAR(50),
                    mean_period_wind_waves FLOAT,
                    mean_period_wind_waves_units VARCHAR(50),
                    mean_period_total_swell FLOAT,
                    mean_period_total_swell_units VARCHAR(50),
                    direction_wind_waves FLOAT,
                    direction_wind_waves_units VARCHAR(50),
                    direction_swell_waves FLOAT,
                    direction_swell_waves_units VARCHAR(50),
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
    def insert_swell_data(self, spot, params, metadata):
        """Inserts extracted surf data into a PostgreSQL database."""

        insert_query = """
        INSERT INTO swell (
            surf_spot_name, country, lat, lng,
            wind_speed, wind_speed_units,
            wind_direction, wind_direction_units,
            u_component_of_wind, u_component_of_wind_units,
            v_component_of_wind, v_component_of_wind_units,
            sig_height_combined_waves, sig_height_combined_waves_units,
            primary_wave_mean_period, primary_wave_mean_period_units,
            primary_wave_direction, primary_wave_direction_units,
            sig_height_wind_waves, sig_height_wind_waves_units,
            sig_height_total_swell, sig_height_total_swell_units,
            mean_period_wind_waves, mean_period_wind_waves_units,
            mean_period_total_swell, mean_period_total_swell_units,
            direction_wind_waves, direction_wind_waves_units,
            direction_swell_waves, direction_swell_waves_units,
            date, time, forecast_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        values_to_insert = (
            spot["name"],
            spot["country"],
            float(spot["lat"]),
            float(spot["lng"]),
            params.get("wind_speed", np.nan),
            params.get("wind_speed_units", "unknown"),
            params.get("wind_direction", np.nan),
            params.get("wind_direction_units", "unknown"),
            params.get("u_component_of_wind", np.nan),
            params.get("u_component_of_wind_units", "unknown"),
            params.get("v_component_of_wind", np.nan),
            params.get("v_component_of_wind_units", "unknown"),
            params.get("sig_height_combined_waves", np.nan),
            params.get("sig_height_combined_waves_units", "unknown"),
            params.get("primary_wave_mean_period", np.nan),
            params.get("primary_wave_mean_period_units", "unknown"),
            params.get("primary_wave_direction", np.nan),
            params.get("primary_wave_direction_units", "unknown"),
            params.get("sig_height_wind_waves", np.nan),
            params.get("sig_height_wind_waves_units", "unknown"),
            params.get("sig_height_total_swell", np.nan),
            params.get("sig_height_total_swell_units", "unknown"),
            params.get("mean_period_wind_waves", np.nan),
            params.get("mean_period_wind_waves_units", "unknown"),
            params.get("mean_period_total_swell", np.nan),
            params.get("mean_period_total_swell_units", "unknown"),
            params.get("direction_wind_waves", np.nan),
            params.get("direction_wind_waves_units", "unknown"),
            params.get("direction_swell_waves", np.nan),
            params.get("direction_swell_waves_units", "unknown"),
            metadata.get("data_date", "1970-01-01"),
            metadata.get("data_time", "00:00:00"),
            metadata.get("forecast_time", "00:00:00"),
        )

        # Replace NaN with None for database insertion
        values_to_insert = [
            None if isinstance(value, float) and np.isnan(value) else value
            for value in values_to_insert
        ]

        try:
            self.cursor.execute(insert_query, values_to_insert)
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()

    # Find the nearest point for a given lat/lon in the GRIB data
    def find_nearest_point(self, latitudes, longitudes, surf_spot_lat, surf_spot_lng):
        """Finds the nearest point in the GRIB data for the given la/lon."""
        lat_lon_pairs = np.column_stack([latitudes.ravel(), longitudes.ravel()])
        tree = cKDTree(lat_lon_pairs)
        _, idx = tree.query([surf_spot_lat, surf_spot_lng])
        return idx

    # Process the GRIB file using pygrib
    def process_grib_file(self, grib_file, surf_spots):
        """Processes the GRIB file for each surf spot and inserts data into Postgres."""
        # Open the GRIB file using pygrib
        grbs = pygrib.open(grib_file)

        for grb in grbs:
            # Extract metadata from the GRIB message
            metadata = self.extract_grib_metadata(grb)

            # Extract and filter data
            values, latitudes, longitudes = self.extract_and_filter_data(grb)

            # Loop over surf spots to extract the nearest value
            for spot in surf_spots:
                idx = self.find_nearest_point(
                    latitudes, longitudes, float(spot["lat"]), float(spot["lng"])
                )
                nearest_value = values.ravel()[idx]

                params = {
                    "wind_speed": None,
                    "wind_direction": None,
                    "u_component_of_wind": None,
                    "v_component_of_wind": None,
                    "sig_height_combined_waves": None,
                    "primary_wave_mean_period": None,
                    "primary_wave_direction": None,
                    "sig_height_wind_waves": None,
                    "sig_height_total_swell": None,
                    "mean_period_wind_waves": None,
                    "mean_period_total_swell": None,
                    "direction_wind_waves": None,
                    "direction_swell_waves": None,
                }

                if grb.name in params:
                    params[grb.name] = nearest_value

                # Insert the surf spot and GRIB data into the database
                self.insert_swell_data(spot, params, metadata)

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
