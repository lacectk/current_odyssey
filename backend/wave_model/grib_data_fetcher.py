import requests


class GribDataFetcher:
    def __init__(
        self, base_url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"
    ):
        self.base_url = base_url

    def generate_grib_url(
        self,
        timestamp,
        model="gfswave",
        region="arctic",
        res="9km",
        forecast_hour="f000",
    ):
        """
        Generates the full URL for a GRIB file using the filter CGI script.
        Adjusts to specify parameters like levels, variables, and directory.
        """
        # Format date and hour strings from the timestamp
        date_str = timestamp.strftime("%Y%m%d")
        hour_str = timestamp.strftime("%H")

        # Construct the directory path based on the timestamp
        dir_path = f"/gfs.{date_str}/{hour_str}/wave/gridded"

        # Correct the filename pattern
        filename = f"{model}.t{hour_str}z.{region}.{res}.{forecast_hour}.grib2"

        # Define the CGI parameters for filtering
        params = {
            "file": filename,
            # "lev_surface": "on",  # Surface level for wave data
            # "var_WVDIR": "on",  # Wave direction
            # "var_WVHT": "on",  # Significant wave height
            # "var_PERPW": "on",  # Primary wave mean period
            "dir": dir_path,
        }

        # Construct the full URL with parameters
        url = requests.Request("GET", self.base_url, params=params).prepare().url
        return url

    def fetch_grib_file(self, url, dest_file):
        """
        Fetches the GRIB file from the specified URL and saves it locally.
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
