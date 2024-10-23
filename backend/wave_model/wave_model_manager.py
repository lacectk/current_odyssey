from backend.wave_model.database_manager import DatabaseManager
from backend.wave_model.grib_data_fetcher import GribDataFetcher
from backend.wave_model.grib_data_processor import GribDataProcessor
from datetime import datetime, timedelta
import json


class WaveModelManager:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.fetcher = GribDataFetcher()
        self.processor = GribDataProcessor(self.db_manager)

    def load_surf_spots(self, json_file):
        with open(json_file, "r") as file:
            surf_spots = json.load(file)
        return surf_spots

    def process_grib_datasets(self, surf_spots, start_date, end_date, interval_hours=6):
        timestamp = start_date
        while timestamp <= end_date:
            url = self.fetcher.generate_grib_url(timestamp)
            local_file = f"grib_{timestamp.strftime('%Y%m%d_%H%M')}.grib2"

            grib_file = self.fetcher.fetch_grib_file(url, local_file)
            if grib_file:
                self.processor.process_grib_file(grib_file, surf_spots)

            timestamp += timedelta(hours=interval_hours)

    def run(self, json_file, start_date, end_date):
        self.db_manager.setup_table()
        surf_spots = self.load_surf_spots(json_file)
        self.process_grib_datasets(surf_spots, start_date, end_date)
        self.db_manager.close()


# Main entry point
def main():
    json_file = "data/surfspots.json"
    start_date = datetime(2024, 10, 1, 0, 0)  # Example start date
    end_date = datetime(2024, 10, 10, 0, 0)  # Example end date

    wave_model_manager = WaveModelManager()
    wave_model_manager.run(json_file, start_date, end_date)


if __name__ == "__main__":
    main()
