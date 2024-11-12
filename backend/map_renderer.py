import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import io
import base64


def generate_map(data):
    fig, ax = plt.subplots(
        figsize=(12, 8), subplot_kw={"projection": ccrs.PlateCarree()}
    )

    ax.coastlines()
    ax.stock_img()

    # Plot points
    for point in data:
        ax.plot(
            point["longitude"],
            point["latitude"],
            "o",
            color=get_consistency_color(point["consistency_label"]),
            transform=ccrs.PlateCarree(),
        )

    # Convert to base64 string
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()

    return base64.b64encode(image_png).decode()
