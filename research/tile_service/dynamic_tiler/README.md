
# Dynamic Tiling Experimentation

This directory contains a set of experimental scripts, images, and a Dash application to test the use of [TiTiler](https://developmentseed.org/titiler/), an open source dyanmic tile server.

The goal is to visualize gridded climate data on a map application.

## How to Use
### 1. Data Generation

In the `/data` directory, there is a small Jupyter notebook that gives an example of processing a NetCDF file using the Xarray package, and outputting a Cloud Optimized GeoTIFF file (COG). A COG is optimal data file type to use with TiTiler, and outputs with a .tif extension.

Use the notebook with your own input NetCDF (or any input of your choice), and output the COG from the Xarray dataset as `OutputCOG.tif` in the `/data` directory.

### 2. Run the TiTiler Stack

First, you will need to have Docker installed on your machine. Please follow [these instructions](https://docs.docker.com/engine/install/) to setup if needed.

Once installed, run the following command in the current directory.

```bash
docker compose up --build
```

This will begin running both the TiTiler application and the custom "file serving" script. The file serving script allows TiTiler to make partial GET requests to our output data file. In production, we would likely store the data on a cloud provider that allows partial GET requests directly to the object. The file serve script is simply for local testing.

### 3. Run the Dash App

Finally, we will run the map application. Navigate to the `/app` directory and install the packages in `requirements.txt` (preferably in a virtual environment or conda environment!)

Next, run the app in a new terminal tab.

```bash
python app.py
```

The app should now be running on your local machine and can be accessed by going to http://127.0.0.1 in your web browser. 