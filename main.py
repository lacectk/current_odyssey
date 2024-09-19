"""Simple PyPi Wrapper for the NOAA NDBC observation data.
Find the list of active buoys at https://www.ndbc.noaa.gov/"""

import aiohttp
import asyncio
from io import StringIO
import json
import matplotlib.pyplot as plt
import pandas as pd
import xmltodict

STATION_URL = "https://www.ndbc.noaa.gov/activestations.xml"
OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"


class NDBC:
    def __init__(
        self,
        station_id: str = None,
        session: aiohttp.ClientSession = None,
    ):
        self._station_id = station_id
        self._session = session or aiohttp.ClientSession()

    async def close(self):
        if not self._session.closed:
            await self._session.close()

    async def get_data(self):
        """Get the observation data and structure to meet defined spec."""
        stations = Stations()
        stations_list = await stations.list()
        await stations.close()

        california_stations = [
            (station_id, station_data)
            for station_id, station_data in stations_list.items()
            if "CA" in station_data.get("@name", "")
        ]

        # Select 5 stations
        selected_stations = california_stations[:5]

        wvht_dict = {}
        # Fetch WVHT data for each selected station
        for station_id, _ in selected_stations:
            self._station_id = station_id
            print(f"Obtaining data for station: {station_id}")
            data = await self.get_station_data()
            if data is not None and not data.empty:
                wvht_dict[station_id] = data

        # Plot WVHT time series for all stations
        self.plot_wvht(wvht_dict)

    async def get_station_data(self):
        """Get observation data from NOAA, process multiple files, and
        combine valid WVHT."""

        # Prepare file variants that we will check for valid data.
        file_variants = [
            f"{self._station_id}.spec",
            f"{self._station_id}.txt",
            f"{self._station_id}.drift",
        ]

        aggregated_df = None
        # Define different column formats for spec and txt files
        spec_columns = [
            "Year",
            "Month",
            "Day",
            "Hour",
            "Minute",
            "WVHT",
            "SwH",
            "SwP",
            "WWH",
            "WWP",
            "SwD",
            "WWD",
            "STEEPNESS",
            "APD",
            "MWD",
        ]
        txt_columns = [
            "Year",
            "Month",
            "Day",
            "Hour",
            "Minute",
            "WDIR",
            "WSPD",
            "GST",
            "WVHT",
            "DPD",
            "APD",
            "MWD",
            "PRES",
            "ATMP",
            "WTMP",
            "DEWP",
            "VIS",
            "PTDY",
            "TIDE",
        ]

        drift_columns = [
            "Year",
            "Month",
            "Day",
            "HourMinute",
            "LAT",
            "LON",
            "WDIR",
            "WSPD",
            "GST",
            "PRES",
            "PTDY",
            "ATMP",
            "WTMP",
            "DEWP",
            "WVHT",
            "DPD",
        ]

        # Iterate over each file variant and attempt to read valid WVHT data
        for file_name in file_variants:
            request_url = f"{OBSERVATION_BASE_URL}{file_name}"
            print(f"Trying file: {file_name}")

            try:
                async with await self._session.get(request_url) as resp:
                    response = await resp.text()

                    if response is not None and resp.status != 404:
                        try:
                            if file_name.endswith(".spec"):
                                columns = spec_columns
                            elif file_name.endswith(".txt"):
                                columns = txt_columns
                            else:
                                columns = drift_columns

                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,
                                sep="\s+",
                                names=columns,
                                engine="python",
                            )

                            if file_name.endswith(".drift"):

                                def extract_hour_minute(hm):
                                    hm_str = str(hm).strip()
                                    if len(hm_str) == 4 and hm_str.isdigit():
                                        return hm_str[:2], hm_str[2:]
                                    elif len(hm_str) == 3 and hm_str.isdigit():
                                        return hm_str[:1], hm_str[2:]
                                    elif len(hm_str) == 2 and hm_str.isdigit():
                                        return 0, hm_str[2:]
                                    else:
                                        return None, None

                                hour_minute_list = df["HourMinute"]
                                hours, minutes = zip(
                                    *[
                                        extract_hour_minute(hm)
                                        for hm in hour_minute_list
                                    ]
                                )
                                df["Hour"] = hours
                                df["Minute"] = minutes

                            # Create a datetime column by combining year,
                            # month, day, hour, and minute
                            df["DateTime"] = pd.to_datetime(
                                df[["Year", "Month", "Day", "Hour", "Minute"]]
                            )

                            # Keep only the necessary columns
                            df = df[["DateTime", "WVHT"]]

                            # Filter out rows with invalid WVHT values
                            #  (non-float or missing "MM")
                            valid_df = df[
                                (~pd.isna(pd.to_numeric(df["WVHT"], errors="coerce")))
                                & (df["WVHT"] != "MM")
                            ]

                            # If there's no valid data in the current file,
                            # skip to the next one
                            if len(valid_df) == 0:
                                print(f"No valid WVHT values in {file_name}")
                                continue

                            # Combine the valid data from the current file
                            # with the rest
                            if aggregated_df is None:
                                aggregated_df = valid_df
                            else:
                                aggregated_df = pd.concat(
                                    [aggregated_df, valid_df]
                                ).drop_duplicates(subset=["DateTime"], keep="first")
                        except Exception as e:
                            print(f"Error while processing data: {e}")
                            return None

            except aiohttp.ClientError as e:
                print(f"HTTP error: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error: {e}")
                continue

        if aggregated_df is not None and not aggregated_df.empty:
            return aggregated_df
        else:
            print("No valid WVHT data found in any files.")
            return None

    def plot_wvht(self, all_data):
        """Plot WVHT data for the selected stations."""
        n = len(all_data)  # Number of stations (subplots)

        # Create subplots with `n` rows and 1 column
        fig, axes = plt.subplots(n, 1, figsize=(12, 8), sharex=True)

        # If there's only one subplot, axes will not be an array, so we handle that
        if n == 1:
            axes = [axes]

        for ax, (station_id, data) in zip(axes, all_data.items()):
            print(station_id)
            print(data)
            data = data.sort_values(by="DateTime")
            ax.plot(data["DateTime"], data["WVHT"], label=f"Station {station_id}")
            ax.set_ylabel("WVHT (m)")
            ax.set_title(f"Station {station_id}")
            ax.legend()
            ax.grid(True)

        # Set the x-label for the last subplot
        axes[-1].set_xlabel("DateTime")

        plt.tight_layout()  # Adjust layout to prevent overlap
        plt.show()


class Stations:
    def __init__(self) -> None:
        self._session = aiohttp.ClientSession()

    async def close(self):
        await self._session.close()

    async def list(self):
        response = ""

        async with await self._session.get(STATION_URL) as resp:
            response = await resp.text()

        try:
            my_dict = xmltodict.parse(response)
            json_data = json.dumps(my_dict)
        except Exception as e:
            raise Exception(f"Error converting Hayward data to JSON: {e}")

        stations = json.loads(json_data)
        list = {}

        for station in stations["stations"]["station"]:
            # Filter stations where the station contains meteorological data.
            if station.get("@met") == "y":
                list[station["@id"]] = station

        return list


async def main():
    # Create an instance of the NDBC class with the station ID
    ndbc = NDBC()
    # Call the get_data method to fetch and print the station data
    await ndbc.get_data()
    # Close the session after use
    await ndbc.close()


if __name__ == "__main__":
    # Use asyncio to run the main function
    asyncio.run(main())
