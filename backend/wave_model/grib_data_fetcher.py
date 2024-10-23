import requests


class GribDataFetcher:
    def __init__(
        self, base_url="https://nomads.ncep.noaa.gov/pub/data/nccf/com/wave/prod/"
    ):
        self.base_url = base_url

    def generate_grib_url(self, timestamp, model="gfswave"):
        date_str = timestamp.strftime("%Y%m%d")
        hour_str = timestamp.strftime("%H")
        file_name = f"{model}.t{hour_str}z.global.0p25.f000.grib2"
        return f"{self.base_url}/{model}.{date_str}/{file_name}"

    def fetch_grib_file(self, url, dest_file):
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
