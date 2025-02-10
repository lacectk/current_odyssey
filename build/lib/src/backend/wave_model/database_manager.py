import psycopg2
import os


class DatabaseManager:
    def __init__(self, db_name="swell"):
        self.conn = psycopg2.connect(
            dbname=db_name,
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        self.cursor = self.conn.cursor()

    def setup_table(self):
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

    def insert_data(self, insert_query, values):
        try:
            self.cursor.execute(insert_query, values)
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()
