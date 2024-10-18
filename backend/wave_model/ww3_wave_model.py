import os
import requests
import xarray as xr
from datetime import datetime

_base_gfs_wave_grib_url = "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{0}/{3}/wave/gridded/{1}.t{3}z.{2}.f{4}.grib2"  # noqa: E501


class WW3DataFetcher:
    def __init__(self, name, subset):
        self.name = name
        self.subset = subset

    def latest_model_time(self):
        """
        Returns the latest available model run time. For simplicity, we use
        the current time minus a few hours. Adjust this logic based on real
        model update frequencies.
        """
        now = datetime.now(datetime.timezone.utc)
        # Assuming model updates every 6 hours: 00, 06, 12, 18
        hour = (now.hour // 6) * 6
        return datetime(now.year, now.month, now.day, hour)

    def create_grib_url(self, time_index):
        """
        Create the NOAA WW3 GRIB URL for the given time index.
        """
        model_run_time = self.latest_model_time()
        model_run_str = str(model_run_time.hour).rjust(2, "0")
        hour_str = str(int(time_index)).rjust(3, "0")
        date_str = model_run_time.strftime("%Y%m%d")
        url = _base_gfs_wave_grib_url.format(
            date_str, self.name, self.subset, model_run_str, hour_str
        )
        return url

    def fetch_ww3_data(self, time_index, save_path):
        """
        Downloads WW3 data for a given forecast time index and saves it to the
        specified path.
        """
        url = self.create_grib_url(time_index)
        print(f"Fetching data from {url}")
        response = requests.get(url)

        if response.status_code == 200:
            with open(save_path, "wb") as file:
                file.write(response.content)
            print(f"Data saved to {save_path}")
        else:
            raise Exception(
                f"Failed to fetch data. Status code: {response.status_code}"
            )

    def load_ww3_data(self, file_path):
        """
        Loads WW3 data from a NetCDF file using xarray.
        """
        print(f"Loading data from {file_path}")
        data = xr.open_dataset(file_path)
        return data


def main():
    # Instantiate the WW3DataFetcher with a specific model name and subset
    ww3_fetcher = WW3DataFetcher(name="gfswave", subset="atlocn")

    # Forecast time index (for example, 64 hours out)
    time_index = 64

    # Define the output file path
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    output_file = os.path.join(data_dir, f"ww3_wave_{time_index}.grib2")

    # Fetch and save the WW3 data
    ww3_fetcher.fetch_ww3_data(time_index, output_file)

    # Load the WW3 data
    ww3_data = ww3_fetcher.load_ww3_data(output_file)
    print(ww3_data)


if __name__ == "__main__":
    main()
