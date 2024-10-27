import os
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from pydap.client import open_url
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get EarthData token from the environment
bearer_token = os.getenv("EARTHDATA_TOKEN")

# Use the exact granule ID obtained from EarthData Search
granule_id = "*_202410*"  # Replace with the actual granule ID. October 2024 for example is *_202410*
collection_id = (
    "C1234567890-PODAAC"  # Replace with actual collection ID for SARAL VAD dataset
)

# Construct the SARAL dataset URL
saral_url = f"https://opendap.earthdata.nasa.gov/collections/{collection_id}/granules/{granule_id}"

# Set up headers for Bearer Token Authorization
auth_header = {"Authorization": f"Bearer {bearer_token}"}

# Create a session and pass the Bearer token for authorization
session = requests.Session()
session.headers.update(auth_header)

# Connect to the SARAL dataset using PyDAP
try:
    dataset = open_url(saral_url, session=session, protocol="dap4")
    print("Connected to SARAL dataset!")

    # Inspect the available variables (e.g., 'sea_surface_height_anomaly')
    dataset.tree()

    # Access a specific variable (e.g., 'sea_surface_height_anomaly')
    ssha = dataset["data_01/sea_surface_height_anomaly"][:]
    longitude = dataset["data_01/longitude"][:]
    latitude = dataset["data_01/latitude"][:]

    # Decode function to handle _FillValue and scaling
    def decode(variable):
        """Decodes the variable according to its attributes."""
        scale_factor = variable.attributes.get("scale_factor", 1)
        fill_value = variable.attributes.get("_FillValue", None)

        data = variable[:]
        if fill_value is not None:
            data = np.where(data == fill_value, np.nan, data)
        return data * scale_factor

    # Decode the sea surface height anomaly, longitude, and latitude
    ssha_data = decode(ssha)
    longitude_data = decode(longitude)
    latitude_data = decode(latitude)

    # Plotting the sea surface height anomaly data
    plt.figure(figsize=(15, 5))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()
    ax.coastlines()
    ax.stock_img()
    plt.scatter(x=longitude_data, y=latitude_data, c=ssha_data, cmap="coolwarm", s=1)
    plt.colorbar(label="Sea Surface Height Anomaly [m]")
    plt.title("SARAL Sea Surface Height Anomaly - October 2024")
    plt.show()

except Exception as e:
    print(f"Failed to connect to SARAL dataset: {e}")
