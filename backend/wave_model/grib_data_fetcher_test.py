import unittest
from datetime import datetime
import requests_mock
from backend.wave_model import GribDataFetcher  # Replace with actual module name


class GribDataFetcherTest(unittest.TestCase):
    def setUp(self):
        self.fetcher = GribDataFetcher()

    def test_generate_grib_url(self):
        timestamp = datetime(2024, 10, 1, 6, 0)
        url = self.fetcher.generate_grib_url(timestamp, model="gfswave")

        expected_url = (
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/wave/prod/"
            "gfswave.20241001/gfswave.t06z.global.0p25.f000.grib2"
        )

        self.assertEqual(url, expected_url)

    @requests_mock.Mocker()
    def test_fetch_grib_file_success(self, mock_requests):
        url = "https://example.com/test.grib2"
        dest_file = "test.grib2"

        # Mock the response
        mock_requests.get(url, content=b"test grib content", status_code=200)

        # Call the fetch method
        fetched_file = self.fetcher.fetch_grib_file(url, dest_file)

        # Assert the file was downloaded successfully
        self.assertEqual(fetched_file, dest_file)
        with open(dest_file, "rb") as f:
            self.assertEqual(f.read(), b"test grib content")

    @requests_mock.Mocker()
    def test_fetch_grib_file_failure(self, mock_requests):
        url = "https://example.com/test.grib2"
        dest_file = "test.grib2"

        # Mock a failed response
        mock_requests.get(url, status_code=404)

        # Call the fetch method
        fetched_file = self.fetcher.fetch_grib_file(url, dest_file)

        # Assert the file was not downloaded
        self.assertIsNone(fetched_file)


if __name__ == "__main__":
    unittest.main()
