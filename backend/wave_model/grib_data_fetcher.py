import requests
from datetime import datetime, timedelta


class GribDataFetcher:
    def __init__(self, base_url="https://nomads.ncep.noaa.gov/dods/"):
        self.base_url = base_url

    def generate_grib_url(
        self, date, run_hour=0, res="0p25", step="3hr", forecast_hour="000"
    ):
        """
        Generates the URL for the GRIB file from the NOAA NOMADS server.
        Args:
            date (datetime): The date of the forecast run.
            run_hour (int): The forecast run hour (0, 6, 12, or 18).
            res (str): The resolution of the forecast (e.g., '0p25', '0p50', '1p00').
            step (str): The timestep ('3hr' or '1hr' for 0p25 resolution).
            forecast_hour (str): The forecast hour (e.g., '000', '003').
        Returns:
            str: Full URL for the GRIB file.
        """
        date_str = date.strftime("%Y%m%d")
        run_str = f"{run_hour:02d}"

        url = f"{self.base_url}gfs_{res}{step}/gfs{date_str}/gfs_{res}{step}_{run_str}z.ascii?gustsfc[0][540][1260]"

        return url

    def fetch_grib_file(self, url, dest_file):
        """
        Fetches the GRIB file from the specified URL and saves it locally.
        Args:
            url (str): URL of the GRIB file to download.
            dest_file (str): Local path to save the downloaded file.
        Returns:
            str: Local file path if successful, None otherwise.
        """
        print(f"Attempting to download: {url}")
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(dest_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {url} successfully.")
                return dest_file
            else:
                print(f"Failed to download {url}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching GRIB file: {e}")
            return None


# Example usage:
if __name__ == "__main__":
    fetcher = GribDataFetcher()
    date = datetime.utcnow() - timedelta(
        days=1
    )  # Use yesterday's date for availability
    url = fetcher.generate_grib_url(
        date=date, run_hour=0, res="0p25", step="3hr", forecast_hour="000"
    )
    fetcher.fetch_grib_file(url, "test.grib2")
