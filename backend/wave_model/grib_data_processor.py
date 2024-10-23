from rtree import index
import numpy as np
import pygrib


class GribDataProcessor:
    def __init__(self, db_manager, rtree_index=index.Index()):
        self.db_manager = db_manager
        self.rtree_index = rtree_index

    def extract_grib_metadata(self, grb):
        metadata = {
            "units": grb.units,
            "data_date": grb.analDate.strftime("%Y%m%d"),
            "data_time": grb.analDate.strftime("%H%M"),
            "forecast_time": f"{grb.forecastTime} hours",
        }
        return metadata

    def extract_and_filter_data(self, grb):
        values = grb.values
        lats, lons = grb.latlons()
        filtered_values = np.where(values == 9999.0, np.nan, values)
        return filtered_values, lats, lons

    def build_rtree(self, latitudes, longitudes):
        for i, (lat, lon) in enumerate(zip(latitudes.ravel(), longitudes.ravel())):
            self.rtree_index.insert(i, (lon, lat, lon, lat))

    def find_nearest_point_rtree(self, surf_spot_lat, surf_spot_lng):
        nearest = list(
            self.rtree_index.nearest(
                (surf_spot_lng, surf_spot_lat, surf_spot_lng, surf_spot_lat), 1
            )
        )
        return nearest[0]

    def process_grib_file(self, grib_file, surf_spots):
        grbs = pygrib.open(grib_file)
        for grb in grbs:
            metadata = self.extract_grib_metadata(grb)
            values, latitudes, longitudes = self.extract_and_filter_data(grb)
            self.build_rtree(latitudes, longitudes)

            for spot in surf_spots:
                idx = self.find_nearest_point_rtree(
                    float(spot["lat"]), float(spot["lng"])
                )
                nearest_value = values.ravel()[idx]
                params = {grb.name: nearest_value}
                self.insert_swell_data(spot, params, metadata)

    def insert_swell_data(self, spot, params, metadata):
        insert_query = """
        INSERT INTO swell (
            surf_spot_name, country, lat, lng,
            wind_speed, wind_speed_units,
            wind_direction, wind_direction_units,
            u_component_of_wind, u_component_of_wind_units,
            v_component_of_wind, v_component_of_wind_units,
            sig_height_combined_waves, sig_height_combined_waves_units,
            primary_wave_mean_period, primary_wave_mean_period_units,
            primary_wave_direction, primary_wave_direction_units,
            sig_height_wind_waves, sig_height_wind_waves_units,
            sig_height_total_swell, sig_height_total_swell_units,
            mean_period_wind_waves, mean_period_wind_waves_units,
            mean_period_total_swell, mean_period_total_swell_units,
            direction_wind_waves, direction_wind_waves_units,
            direction_swell_waves, direction_swell_waves_units,
            date, time, forecast_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        values_to_insert = (
            spot["name"],
            spot["country"],
            float(spot["lat"]),
            float(spot["lng"]),
            params.get("wind_speed", None),
            params.get("wind_speed_units", "unknown"),
            params.get("wind_direction", None),
            params.get("wind_direction_units", "unknown"),
            params.get("u_component_of_wind", None),
            params.get("u_component_of_wind_units", "unknown"),
            params.get("v_component_of_wind", None),
            params.get("v_component_of_wind_units", "unknown"),
            params.get("sig_height_combined_waves", None),
            params.get("sig_height_combined_waves_units", "unknown"),
            params.get("primary_wave_mean_period", None),
            params.get("primary_wave_mean_period_units", "unknown"),
            params.get("primary_wave_direction", None),
            params.get("primary_wave_direction_units", "unknown"),
            params.get("sig_height_wind_waves", None),
            params.get("sig_height_wind_waves_units", "unknown"),
            params.get("sig_height_total_swell", None),
            params.get("sig_height_total_swell_units", "unknown"),
            params.get("mean_period_wind_waves", None),
            params.get("mean_period_wind_waves_units", "unknown"),
            params.get("mean_period_total_swell", None),
            params.get("mean_period_total_swell_units", "unknown"),
            params.get("direction_wind_waves", None),
            params.get("direction_wind_waves_units", "unknown"),
            params.get("direction_swell_waves", None),
            params.get("direction_swell_waves_units", "unknown"),
            metadata.get("data_date", "1970-01-01"),
            metadata.get("data_time", "00:00:00"),
            metadata.get("forecast_time", "00:00:00"),
        )

        values_to_insert = [
            None if value is None else value for value in values_to_insert
        ]
        self.db_manager.insert_data(insert_query, values_to_insert)
