import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import psycopg2
from backend.stations.ndbc_stations_data import NDBCDataFetcher
from your_module import StationsFetcher, Station


class TestStationsFetcher(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        self.stations_fetcher = StationsFetcher()

        # Mock the database connection and cursor
        self.stations_fetcher.conn = MagicMock(spec=psycopg2.extensions.connection)
        self.stations_fetcher.cursor = MagicMock(spec=psycopg2.extensions.cursor)

    @patch.object(NDBCDataFetcher, "fetch_station_data", new_callable=AsyncMock)
    async def test_meteorological_stations(self, mock_fetch_station_data):
        """Test the meteorological_stations method."""
        mock_data = {
            "stations": {
                "station": [
                    {"@id": "001", "@lat": "40.0", "@lon": "-74.0", "@met": "y"},
                    {"@id": "002", "@lat": "41.0", "@lon": "-75.0", "@met": "n"},
                    {"@id": "003", "@lat": "42.0", "@lon": "-76.0", "@met": "y"},
                ]
            }
        }
        mock_fetch_station_data.return_value = mock_data

        stations = await self.stations_fetcher.meteorological_stations()

        # Assert results
        self.assertIn("001", stations)
        self.assertIn("003", stations)
        self.assertNotIn("002", stations)
        self.assertEqual(stations["001"].latitude, 40.0)
        self.assertEqual(stations["001"].longitude, -74.0)

    def test_setup_database(self):
        """Test the setup_database method."""
        self.stations_fetcher.setup_database()

        # Assert that the cursor's execute method was called to create the table
        self.stations_fetcher.cursor.execute.assert_called_once()
        self.stations_fetcher.conn.commit.assert_called_once()

    @patch.object(StationsFetcher, "insert_into_database", new_callable=AsyncMock)
    async def test_insert_into_database(self, mock_insert):
        """Test the insert_into_database method."""
        # Set up mock stations
        station_list = {
            "001": Station("001", 40.0, -74.0),
            "003": Station("003", 42.0, -76.0),
        }

        # Call the insert method
        await self.stations_fetcher.insert_into_database(station_list)

        # Assert that the cursor executed the correct insert statements
        self.stations_fetcher.cursor.execute.assert_any_call(
            """
            INSERT INTO stations (station_id, latitude, longitude)
            VALUES (%s, %s, %s)
            ON CONFLICT (station_id) DO NOTHING;
            """,
            ("001", 40.0, -74.0),
        )
        self.stations_fetcher.cursor.execute.assert_any_call(
            """
            INSERT INTO stations (station_id, latitude, longitude)
            VALUES (%s, %s, %s)
            ON CONFLICT (station_id) DO NOTHING;
            """,
            ("003", 42.0, -76.0),
        )
        self.stations_fetcher.conn.commit.assert_called_once()

    def tearDown(self):
        """Clean up after tests."""
        self.stations_fetcher.cursor.close()
        self.stations_fetcher.conn.close()


if __name__ == "__main__":
    unittest.main()
