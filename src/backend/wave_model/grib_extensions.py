import pygrib
import os


class GribExtenions:
    def find_available_parameters(self, grib_file):
        """
        Finds and prints the available parameters (variables) in the GRIB file.

        Args:
            grib_file (str): Path to the GRIB file.

        Returns:
            List of available parameter names (variables).
        """
        grbs = pygrib.open(grib_file)

        available_parameters = []

        for grb in grbs:
            param = grb.name
            available_parameters.append(param)
            print(f"Available parameter: {param}")

        grbs.close()
        return available_parameters


def main():
    grib_extensions = GribExtenions()

    grib_file = os.path.join(
        os.getenv("HOME"), "Downloads/gfswave.t06z.arctic.9km.f000.grib2"
    )
    grib_extensions.find_available_parameters(grib_file)


if __name__ == "__main__":
    main()
