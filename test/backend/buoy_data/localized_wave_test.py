import aiohttp
import pandas as pd
import unittest
from sqlalchemy import create_engine, Table, MetaData, select, Column, String, Float
from src.backend.buoy_data.localized_wave import LocalizedWaveProcessor
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch, MagicMock

MOCK_SPEC_TXT_DATA = AsyncMock(
    return_value=(
        "YY MM DD hh mm WVHT DPD APD MWD\n"
        "yr mo dy hr mn m sec sec degT\n"
        "2024 12 15 12 30 1.5 8 10 134\n"
        "2024 12 15 13 00 2.0 7 9 145"
    )
)

MOCK_DRIFT_DATA = AsyncMock(
    return_value=(
        "Year Month Day HourMinute LAT LON WDIR WSPD GST PRES ATMP WTMP DEWP WVHT DPD\n"
        "yr mo dy hrmn deg deg degT m/s m/s hPa degC degC degC m sec\n"
        "2024 12 15 1230 35.0 -120.5 300 12 15 1013 18.5 16.2 10.0 1.5 8\n"
        "2024 12 15 1230 35.0 -120.5 300 14 17 1015 20.5 18.2 11.0 1.7 10\n"
    )
)


class LocalizedWaveProcessorTest(IsolatedAsyncioTestCase):
    def setUp(self):
        """Set up a LocalizedWaveProcessor instance for testing."""
        self.mock_engine = create_engine("sqlite:///:memory:")
        self.processor = LocalizedWaveProcessor()
        self.processor.engine = self.mock_engine
        self.processor.create_wave_table()

        # Create stations table using SQLAlchemy
        metadata = MetaData()
        self.stations_table = Table(
            "stations",
            metadata,
            *[
                Column("station_id", String(10), primary_key=True),
                Column("latitude", Float, nullable=False),
                Column("longitude", Float, nullable=False),
            ],
        )
        metadata.create_all(self.mock_engine)

        # Insert mock data using SQLAlchemy
        with self.mock_engine.connect() as conn:
            conn.execute(
                self.stations_table.insert().values(
                    [
                        {"station_id": "123", "latitude": 35.0, "longitude": -120.5},
                        {"station_id": "678", "latitude": 36.0, "longitude": -121.5},
                    ]
                )
            )
            conn.commit()

            # Verify data insertion
            result = conn.execute(select(self.stations_table)).fetchall()
            print(f"Stations table content: {result}")

    @staticmethod
    def _mock_file_response(url, *args, **kwargs):
        if url.endswith(".drift"):
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.text = MOCK_DRIFT_DATA
            return mock_response
        elif url.endswith(".spec") or url.endswith(".txt"):
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.text = MOCK_SPEC_TXT_DATA
            return MagicMock(__aenter__=AsyncMock(return_value=mock_response))
        else:
            mock_response = MagicMock()
            mock_response.status = 404
            mock_response.text = AsyncMock(return_value="")
            return MagicMock(__aenter__=AsyncMock(return_value=mock_response))

    @patch("aiohttp.ClientSession.get")
    async def test_fetch_station_data(self, mock_get):
        mock_get.side_effect = self._mock_file_response

        station_id = "123"

        async with aiohttp.ClientSession() as session:
            data, lat, lon = await self.processor._fetch_station_data(
                session=session, station_id=station_id
            )

        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 2)
        self.assertIn("datetime", data.columns)
        self.assertEqual(data["wvht"].iloc[0], 1.5)  # Using lowercase column names
        self.assertEqual(lat, None)
        self.assertEqual(lon, None)

    @patch("aiohttp.ClientSession.get")
    @patch(
        "src.backend.buoy_data.localized_wave.LocalizedWaveProcessor._insert_localized_wave_data_into_db",
        new_callable=AsyncMock,
    )
    async def test_fetch_stations_data(self, mock_insert_data, mock_get):
        """Test fetching wave data for a list of stations."""
        mock_get.side_effect = self._mock_file_response

        async def side_effect(*args, **kwargs):
            original_method = LocalizedWaveProcessor._insert_localized_wave_data_into_db
            return await original_method(self.processor, *args, **kwargs)

        mock_insert_data.side_effect = lambda station_id, data, lat, lon: side_effect(
            self.processor, station_id, data, lat, lon
        )

        station_ids = ["123", "678"]
        self.processor.station_ids = station_ids

        await self.processor.fetch_stations_data()
        print(f"Mock insert data calls: {mock_insert_data.mock_calls}")

        self.assertEqual(mock_insert_data.call_count, 2)

    @patch("src.backend.buoy_data.localized_wave.select")
    async def test_insert_localized_wave_data_when_stationsdb_needed(self, mock_select):
        """Test inserting data into the database."""
        data = pd.DataFrame(
            {
                "datetime": ["2024-12-15 12:30:00"],
                "wvht": [1.5],
                "dpd": [8],
                "apd": [10],
                "mwd": ["NNE"],
            }
        )
        station_id = "123"
        lat, lon = None, None

        with patch("sqlalchemy.engine.base.Connection.execute") as mock_execute:
            await self.processor._insert_localized_wave_data_into_db(
                station_id=station_id, data=data, lat=lat, lon=lon
            )

        mock_select.assert_called_once()
        self.assertTrue(mock_execute.called, "Database execute was not called.")

    @patch("src.backend.buoy_data.localized_wave.select")
    async def test_insert_localized_wave_data_when_lat_lon_in_drift(self, mock_select):
        """Test inserting data into the database."""
        data = pd.DataFrame(
            {
                "datetime": ["2024-12-15 12:30:00"],
                "wvht": [1.5],
                "dpd": [8],
                "apd": [10],
                "mwd": ["NNE"],
            }
        )
        station_id = "123"
        lat, lon = 35.0, -120.5

        with patch("sqlalchemy.engine.base.Connection.execute") as mock_execute:
            await self.processor._insert_localized_wave_data_into_db(
                station_id=station_id, data=data, lat=lat, lon=lon
            )

        self.assertTrue(mock_execute.called, "Database execute was not called.")

    def tearDown(self):
        """Clean up resources."""
        self.processor.engine.dispose()


if __name__ == "__main__":
    unittest.main()
