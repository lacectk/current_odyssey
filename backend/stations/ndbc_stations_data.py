import aiohttp
import json
import xmltodict

STATION_URL = "https://www.ndbc.noaa.gov/activestations.xml"


class NDBCDataFetcher:
    def __init__(self):
        self._session = aiohttp.ClientSession()

    async def fetch_station_data(self):
        async with self._session.get(STATION_URL) as resp:
            response = await resp.text()

        try:
            stations_data = xmltodict.parse(response)
            stations_json = json.dumps(stations_data)
            stations_data = json.loads(stations_json)
        except Exception as e:
            raise Exception(f"Error parsing station data: {e}")

        return stations_data

    async def close(self):
        await self._sessions.close()
