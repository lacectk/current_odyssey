import asyncio
from backend.create_database import create_database
from backend.stations.ndbc_stations_data import NDBCDataFetcher
import os
import psycopg2


class Station(object):
    def __init__(self, station_id, latitude, longitude):
        self.station_id = station_id
        self.latitude = latitude
        self.longitude = longitude


class Stations:
    def __init__(self):
        self.fetcher = NDBCDataFetcher()
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        self.cursor = self.conn.cursor()

    async def meteorological_stations(self):
        stations_data = await self.fetcher.fetch_station_data()
        stations_list = {}

        for station in stations_data["stations"]["station"]:
            if station.get("@met") == "y":
                station_id = station["@id"]
                latitude = float(station["@lat"])
                longitude = float(station["@lon"])

                station_obj = Station(station_id, latitude, longitude)
                stations_list[station_id] = station_obj

        return stations_list

    def setup_database(self):
        try:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stations (
                                station_id VARCHAR PRIMARY KEY,
                                latitude FLOAT NOT NULL,
                                longitude FLOAT NOT NULL
                                );
            """
            )
            self.conn.commit()
            print("Table 'stations' checked/created successfully.")

        except Exception as e:
            print(f"Error setting up the database: {e}")
            self.conn.rollback()

    async def insert_into_database(self, station_list):
        for station_id, station in station_list.items():
            try:
                self.cursor.execute(
                    """
                    INSERT INTO stations (station_id, latitude, longitude)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (station_id) DO NOTHING;
                """,
                    (station.station_id, station.latitude, station.longitude),
                )

                if self.cursor.rowcount > 0:
                    print(f"Inserted new station {station_id}")

            except Exception as e:
                print(f"Error inserting data for station {station_id}: {e}")
                self.conn.rollback()

        self.conn.commit()

    async def close(self):
        self.cursor.close()
        self.conn.close()


async def main():
    create_database("stations")
    # Create an instance of the Stations class and fetch the data
    stations = Stations()
    stations.setup_database()
    station_list = await stations.meteorological_stations()

    # Fetch and insert the station data into the PostgreSQL database
    await stations.insert_into_database(station_list)

    # Close the session
    await stations.close()


if __name__ == "__main__":
    asyncio.run(main())
