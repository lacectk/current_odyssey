import aiohttp
import asyncio
from backend.create_database import create_database
from backend.stations.stations import StationsFetcher
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
import os
import psycopg2

OBSERVATION_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/"


class WaveDataFetcher:
    def __init__(self, station_ids):
        """Initialize with a list of station IDs."""
        self.station_ids = station_ids
        self._session = aiohttp.ClientSession()
        self.conn = psycopg2.connect(
            dbname="wave_data",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        self.cursor = self.conn.cursor()

    async def close(self):
        """Close the session and database connection."""
        await self._session.close()
        self.cursor.close()
        self.conn.close()

    async def fetch_station_wave_data(self):
        """Fetch wave data for each station in the input list."""
        for station_id in self.station_ids:
            print(f"Fetching wave data for station: {station_id}")
            data = await self.get_station_data(station_id)
            if data is not None and not data.empty:
                await self.insert_wave_data_into_db(station_id, data)

    async def get_station_data(self, station_id):
        """Get wave observation data from NOAA and return valid data."""
        file_variants = [
            f"{station_id}.spec",
            f"{station_id}.txt",
            f"{station_id}.drift",
        ]

        # Define different column formats for spec, txt, and drift files
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
            "ATMP",
            "WTMP",
            "DEWP",
            "WVHT",
            "DPD",
        ]

        all_dfs = []
        # Try each file variant and merge the results based on DateTime
        for file_name in file_variants:
            request_url = f"{OBSERVATION_BASE_URL}{file_name}"
            try:
                async with self._session.get(request_url) as resp:
                    if resp.status == 200:
                        response = await resp.text()
                        try:
                            # Determine column format based on file type
                            columns = (
                                spec_columns
                                if file_name.endswith(".spec")
                                else (
                                    txt_columns
                                    if file_name.endswith(".txt")
                                    else drift_columns
                                )
                            )

                            # Read the data into a DataFrame
                            df = pd.read_csv(
                                StringIO(response),
                                skiprows=2,
                                sep="\s+",
                                names=columns,
                                engine="python",
                                na_values=["MM"],
                            )

                            # Handle 'drift' file special column
                            if file_name.endswith(".drift"):
                                df["Hour"] = df["HourMinute"].str[:2]
                                df["Minute"] = df["HourMinute"].str[2:]

                            # Create the 'DateTime' column
                            df["DateTime"] = pd.to_datetime(
                                df[["Year", "Month", "Day", "Hour", "Minute"]],
                                errors="coerce",
                            )

                            # Append the dataframe to the list for merging later
                            all_dfs.append(df)

                        except Exception as e:
                            print(f"Error processing {file_name}: {e}")
            except Exception as e:
                print(f"Failed to fetch data for {file_name}: {e}")
                continue

        # Merge dataframes based on DateTime, allowing partial data to combine
        if all_dfs:
            concatenated_df = pd.concat(all_dfs)
            # Select only relevant wave columns
            wave_columns = ["DateTime", "WVHT", "DPD", "APD", "MWD"]
            aggregated_df = (
                concatenated_df[wave_columns].groupby("DateTime").first().reset_index()
            )

            # Ensure we drop rows with invalid DateTime (NaT)
            aggregated_df = aggregated_df.dropna(subset=["DateTime"])

            if not aggregated_df.empty:
                return aggregated_df
            else:
                print(
                    f"No valid wave data found in any files for station: {station_id}."
                )
                return None

    def setup_wave_data_table(self):
        """Create the wave_data table if it doesn't exist."""
        try:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS wave_data (
                    station_id VARCHAR NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    WVHT FLOAT,
                    DPD FLOAT,
                    APD FLOAT,
                    MWD FLOAT,
                    PRIMARY KEY (station_id, datetime)
                );
                """
            )
            self.conn.commit()
            print("Table 'wave_data' checked/created successfully.")
        except Exception as e:
            print(f"Error setting up the database: {e}")
            self.conn.rollback()

    async def insert_wave_data_into_db(self, station_id, data):
        """Insert wave data into the database."""
        try:
            for _, row in data.iterrows():
                if pd.isnull(row["DateTime"]):
                    print(
                        "Skipping row with NaT in DateTime for"
                        + "station {station_id}: {row}"
                    )
                    continue  # Skip rows where DateTime is NaT
                # Use None for missing columns (e.g., DPD, APD, MWD)
                wvht = row["WVHT"] if "WVHT" in data.columns else None
                dpd = row["DPD"] if "DPD" in data.columns else None
                apd = row["APD"] if "APD" in data.columns else None
                mwd = row["MWD"] if "MWD" in data.columns else None

            for _, row in data.iterrows():
                self.cursor.execute(
                    """
                    INSERT INTO wave_data (station_id, datetime, WVHT, DPD, APD, MWD)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, datetime) DO NOTHING;
                    """,
                    (
                        station_id,
                        row["DateTime"],
                        wvht,
                        dpd,
                        apd,
                        mwd,
                    ),
                )
            self.conn.commit()
            print(f"Wave data inserted for station {station_id}")
        except Exception as e:
            print(f"Error inserting wave data for station {station_id}: {e}")
            self.conn.rollback()


async def main():
    load_dotenv()  # Load environment variables from .env file
    create_database("wave_data")

    stations = StationsFetcher()
    station_id_list = stations.fetch_station_ids()

    fetcher = WaveDataFetcher(station_id_list)
    fetcher.setup_wave_data_table()  # Set up the database table
    await fetcher.fetch_station_wave_data()  # Fetch wave data for all stations
    await fetcher.close()  # Close the session and database connection


if __name__ == "__main__":
    asyncio.run(main())