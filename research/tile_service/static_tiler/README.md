# Static Tiler

## Overview

Static Tiler is a tool designed to transform GeoTIFF files into static tile files (.png), optimized for use in mapping applications. Leveraging GDAL processes and the Python GDAL API, it outputs tiles in the desired coordinate reference system for overlay in web maps.

This method offers a performance advantage over dynamic tiling by pre-generating tiles, allowing for access and display. However, it does not provide direct access to the underlying climate data encoded in the GeoTIFF. The tile generation is a comprehensive batch process, which, depending on the dataset size and the maximum zoom level (tested up to 11), can take extensive processing time, up to 48 hours.

## Prerequisites

- GDAL: Ensure GDAL is installed and properly configured on your system.
- Python: A Python environment with the GDAL bindings installed.
- Conda (Recommended): For ease of environment management, it's recommended to use Conda. An `environment.yaml` file should be used to create an environment with all necessary dependencies.

## Installation

1. Clone the repository or download the source code.
2. If using Conda, create a new environment:
   ```bash
   conda env create -f environment.yaml
   conda activate static-tiler-env
   ```
3. Ensure GDAL is installed and accessible in your environment.

## Usage

To use Static Tiler, run the following command in your terminal, substituting the placeholders with your specific file paths and desired settings:

## Parameters
`python static_tiler.py --input_file /path/to/data/climate.tif --color_file /path/to/col.txt --output_crs EPSG_CODE --max_zoom_level MAX_ZOOM --output_dir /path/to/output`

- `--input_file`: Path to the input GeoTIFF file.
- `--color_file`: Path to the color file (.txt) for tile styling.
- `--output_crs`: The EPSG code of the desired output coordinate reference system.
- `--max_zoom_level`: The maximum zoom level for which to generate tiles.
- `--output_dir`: The directory where the generated tiles will be stored. Defaults to ./tiles.