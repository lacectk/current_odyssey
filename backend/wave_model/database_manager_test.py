import unittest
from unittest.mock import patch, MagicMock, call
from backend.wave_model import DatabaseManager


class DatabaseManagerTest(unittest.TestCase):
    @patch("psycopg2.connect")
    def setUp(self, mock_connect):
        # Mock database connection
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        mock_connect.return_value = self.mock_conn
        self.mock_conn.cursor.return_value = self.mock_cursor

        self.db_manager = DatabaseManager()

    def test_setup_table(self):
        # Call the method
        self.db_manager.setup_table()

        # Check if the table creation SQL was executed
        self.mock_cursor.execute.assert_called_once_with(
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

        # Ensure commit was called
        self.mock_conn.commit.assert_called_once()

    def test_insert_data(self):
        insert_query = "INSERT INTO swell (surf_spot_name) VALUES (%s);"
        values = ("test_spot",)

        # Call the method
        self.db_manager.insert_data(insert_query, values)

        # Check if the insert statement was executed
        self.mock_cursor.execute.assert_called_once_with(insert_query, values)

        # Ensure commit was called
        self.mock_conn.commit.assert_called_once()

    def test_close(self):
        # Call the close method
        self.db_manager.close()

        # Ensure cursor and connection were closed
        self.mock_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
