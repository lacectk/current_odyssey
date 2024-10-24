import requests


class GribDataFetcher:
    def __init__(
        self, base_url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl"
    ):
        self.base_url = base_url

    def generate_grib_url(
        self, timestamp, model="gfs", res="1p00", forecast_hour="f000"
    ):
        """
        Generates the full URL for a GRIB file using the filter CGI script.
        Adjusts to specify parameters like levels, variables, and directory.
        """
        # Format date and hour strings from the timestamp
        date_str = timestamp.strftime("%Y%m%d")
        hour_str = timestamp.strftime("%H")

        # Construct the directory path based on the timestamp
        dir_path = f"/{model}.{date_str}/{hour_str}/atmos"

        # Define the CGI parameters for filtering
        params = {
            "file": f"{model}.t{hour_str}z.pgrb2.{res}.{forecast_hour}",
            "lev_10_m_above_ground": "on",  # 10-meter level
            "var_UGRD": "on",  # u-component of wind
            "var_VGRD": "on",  # v-component of wind
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


# class GribDataFetcher:
#     def __init__(
#         self, base_url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl"
#     ):
#         self.base_url = base_url

#     def generate_grib_url(
#         self, timestamp, model="gfs", res="1p00", forecast_hour="f000"
#     ):
#         """
#         Generates the full URL for a GRIB file using the filter CGI script.
#         Adjusts to specify parameters like levels, variables, and directory.
#         """
#         # Format date and hour strings from the timestamp
#         date_str = timestamp.strftime("%Y%m%d")
#         hour_str = timestamp.strftime("%H")

#         # Construct the directory path based on the timestamp
#         dir_path = f"/{model}.{date_str}{hour_str}"

#         # Construct the full URL with parameters
#         url = (
#             requests.Request("GET", self.base_url, params={"dir": dir_path})
#             .prepare()
#             .url
#         )
#         return url

#     def fetch_grib_file(self, url, dest_file):
#         """
#         Fetches the GRIB file from the specified URL and saves it locally.
#         """
#         try:
#             response = requests.get(url, stream=True)
#             if response.status_code == 200:
#                 with open(dest_file, "wb") as f:
#                     f.write(response.content)
#                 print(f"Downloaded {url} successfully.")
#                 return dest_file
#             else:
#                 print(f"Failed to download {url}. Status code: {response.status_code}")
#                 return None
#         except Exception as e:
#             print(f"Error fetching GRIB file: {e}")
#             return None
