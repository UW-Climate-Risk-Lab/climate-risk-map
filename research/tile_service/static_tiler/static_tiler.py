import argparse
from osgeo import gdal
import subprocess
import subprocess

def remove_file_extension(file_name: str) -> str:
    """
    Remove the file extension from the given file name.

    Args:
        file_name (str): The file name with extension.

    Returns:
        str: The file name without extension.
    """
    return file_name.rsplit(".", 1)[0]

def check_crs(crs: str) -> bool:
    """
    Check if the input CRS is an integer.

    Args:
        crs (str): The input CRS.

    Returns:
        bool: True if the input CRS is an integer, False otherwise.
    """
    try:
        int(crs)
        return True
    except ValueError:
        return False

def main(
    input_file: str,
    output_dir: str,
    color_file: str,
    output_crs: str,
    max_zoom_level: str,
):
    if not check_crs(output_crs):
        raise ValueError('CRS must be an integer!')
    
    filename = remove_file_extension(input_file)
    batchVRT = gdal.BuildVRT(f"{filename}.vrt", input_file)
    batchVRT.FlushCache()

    gdal.Warp(
        destNameOrDestDS=f"{filename}_{output_crs}.vrt",
        srcDSOrSrcDSTab=f"{filename}.vrt",
        dstSRS=f"EPSG:{output_crs}",
    )

    gdal.Translate(
        srcDS=f"{filename}_{output_crs}.vrt",
        destName=f"{filename}_{output_crs}.tif",
        format="GTiff",
        creationOptions=["TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES"],
    )

    subprocess.run(["gdaldem", "color-relief", f"{filename}_{output_crs}.tif", color_file, f"{filename}_{output_crs}_color.tif"])

    gdal.Translate(
        destName=f"{filename}_{output_crs}_color.vrt",
        srcDS=f"{filename}_{output_crs}_color.tif",
        format="VRT",
        outputType=gdal.GDT_Byte,
        scaleParams=[],
    )
    subprocess.run([
        "gdal2tiles.py",
        "-z", f"0-{max_zoom_level}",
        "-w", "leaflet",
        "-s", f"EPSG:{output_crs}",
        "--processes=6",
        "--xyz",
        f"{filename}_{output_crs}_color.vrt",
        output_dir
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GeoTIFF into static tiles.")
    parser.add_argument("--input_file", type=str, help="Input file GeoTIFF path")
    parser.add_argument(
        "--output_dir",
        type=str,
        help="Directory to store output .png files",
        default="./tiles",
    )
    parser.add_argument(
        "--color_file", type=str, help="Path to .txt file to use for color scheme"
    )
    parser.add_argument(
        "--output_crs",
        type=str,
        help="Output Coordinate Reference System (CRS) as EPSG code",
        default="3857",
    )
    parser.add_argument(
        "--max_zoom_level",
        type=str,
        help="Maximum zoom level to generate.",
        default="5",
    )

    args = parser.parse_args()
    main(
        args.input_file,
        args.output_dir,
        args.color_file,
        args.output_crs,
        args.max_zoom_level,
    )
