import os

import geopandas as gpd
import pandas as pd
import numpy as np


MEDIAN_INCOME_DIRECTORY = "./data/zipcode_median_income"
ZIPCODE_SHAPE_FILE = "./data/zipcode_shape_file/tl_2020_us_zcta520.shp"
ELECTRIC_RETAIL_SERVICE_AREA_FILE = "./data/Electric_Retail_Service_Territories.geojson"

CRS = "EPSG:4269"
STATES = ["WA", "CA", "OR"]


def load_median_income_dataframe() -> pd.DataFrame:
    """
    Reads in median income CSV files and concats

    *Columns*
    zipcode (int64): Zipcode number
    N1 (float64): Number of persons recorded for avg AGI
    A00100 (float64): Adjusted gross income (in thousands)
    """
    data = []
    for file in os.listdir(MEDIAN_INCOME_DIRECTORY):
        if file.endswith(".csv"):
            df_temp = pd.read_csv(f"{MEDIAN_INCOME_DIRECTORY}/{file}")
            df_temp = df_temp.loc[df_temp["STATE"].isin(STATES)].copy()
            df_temp["year"] = int(f"20{file[0:2]}")
            data.append(df_temp[["year", "zipcode", "N1", "A00100"]].copy())
    df = pd.concat(data)
    df["zipcode"] = df["zipcode"].astype(int)

    # Takes weighted average to approximate median income by zipcode per year
    s = df.groupby(["zipcode", "year"]).apply(
        lambda x: sum(x["A00100"] * 1000) / sum(x["N1"]), include_groups=False
    )
    s.name = "weighted_average_agi"
    df = s.reset_index()
    df = df.dropna()
    
    df = df.set_index("zipcode")
    return df


def load_zipcode_geodataframe() -> gpd.GeoDataFrame:

    columns_to_keep = ["ZCTA5CE20", "geometry"]

    gdf_zip_code_raw: gpd.GeoDataFrame = gpd.read_file(ZIPCODE_SHAPE_FILE)
    gdf_zip_code = gdf_zip_code_raw.to_crs(CRS)
    gdf_zip_code["ZCTA5CE20"] = gdf_zip_code_raw["ZCTA5CE20"].astype(int)
    gdf_zip_code = gdf_zip_code.dropna()

    gdf_zip_code = (
        gdf_zip_code[columns_to_keep].copy().rename(columns={"ZCTA5CE20": "zipcode"})
    )

    gdf_zip_code = gdf_zip_code.set_index("zipcode")

    return gdf_zip_code


def load_elec_service_area_geodataframe() -> gpd.GeoDataFrame:

    gdf_elec_service_areas_raw: gpd.GeoDataFrame = gpd.read_file(ELECTRIC_RETAIL_SERVICE_AREA_FILE)
    gdf_elec_service_areas = gdf_elec_service_areas_raw.loc[gdf_elec_service_areas_raw["STATE"].isin(STATES)].copy()
    gdf_elec_service_areas = gdf_elec_service_areas.to_crs(CRS)
    return gdf_elec_service_areas


def calc_zipcode_area_in_utility(
    gdf_zipcode: gpd.GeoDataFrame, gdf_elec_service_area: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    
    gdf_zipcode_geoms = gdf_zipcode[["geometry"]].unique().copy()
    gdf_elec_utility_geoms = gdf_elec_service_area[["geometry"]].unqiue().copy()

    gpd.overlay(
        gdf_zipcode_geoms, gdf_elec_utility_geoms, how="intersection", keep_geom_type=True
    )



    pass


def main():

    df_median_income = load_median_income_dataframe()
    gdf_zipcode = load_zipcode_geodataframe()
    gdf_elec_service_area = load_elec_service_area_geodataframe()

    gdf = calc_zipcode_area_in_utility(gdf_zipcode, gdf_elec_service_area)

    # gdf_elec_service_areas = gdf_elec_service_areas.sjoin(gdf_zip_code, how='inner', predicate="intersects")
    # gdf_elec_service_areas

    # --- 2. Calculate Original Zip Code Areas ---
    # Add a new column to gdf_zipcodes for their original area
    gdf_zip_code["original_zip_area"] = gdf_zip_code.geometry.area

    # --- 3. Perform an Intersection Overlay ---
    # This creates new polygons representing the overlap.
    # Attributes from both GeoDataFrames are carried over to the intersection parts.
    print("\nPerforming overlay (intersection)...")
    intersection_gdf = gpd.overlay(
        gdf_elec_service_area, gdf_zip_code, how="intersection", keep_geom_type=True
    )

    # --- 4. Calculate Intersection Areas ---
    intersection_gdf["intersection_area"] = intersection_gdf.geometry.area

    # --- 5. Calculate Percentage of Zip Code in Service Area ---
    # This is the proportion of the *original* zip code's area that this intersection represents.
    intersection_gdf["percent_of_zip_in_sa"] = 0.0  # Initialize
    # Avoid division by zero if any original_zip_area is 0 or very small
    valid_areas = intersection_gdf["original_zip_area"] > 1e-9  # Use a small threshold
    intersection_gdf.loc[valid_areas, "percent_of_zip_in_sa"] = (
        intersection_gdf.loc[valid_areas, "intersection_area"]
        / intersection_gdf.loc[valid_areas, "original_zip_area"]
    )

    # Ensure percentage is between 0 and 1 (due to potential floating point inaccuracies)
    intersection_gdf["percent_of_zip_in_sa"] = intersection_gdf[
        "percent_of_zip_in_sa"
    ].clip(0, 1)

    # --- 6. Weighted Average ---
    # Now calculate the weighted average median income for each utility service area.
    # The weight is the 'intersection_area' itself or the 'percent_of_zip_in_sa' applied to some base.

    # Option 6a: Using intersection_area directly as the weight for median_income
    # This is common and straightforward: sum(median_income * intersection_area) / sum(intersection_area)
    intersection_gdf["weighted_average_adjusted_gross_income_x_intersection_area"] = (
        intersection_gdf["weighted_average_adjusted_gross_income"]
        * intersection_gdf["intersection_area"]
    )

    utility_area_weighted_income = (
        intersection_gdf.groupby("NAME")
        .agg(
            sum_income_x_intersection_area=(
                "weighted_average_adjusted_gross_income_x_intersection_area",
                "sum",
            ),
            total_intersection_area_in_utility=("intersection_area", "sum"),
        )
        .reset_index()
    )

    utility_area_weighted_income["area_weighted_adjuted_gross_income"] = (
        utility_area_weighted_income["sum_income_x_intersection_area"]
        / utility_area_weighted_income["total_intersection_area_in_utility"]
    )

    print("\nArea-weighted median income per utility service area:")
    print(utility_area_weighted_income.head())

    # Merge this back to your original utility GeoDataFrame
    gdf_utilities_final = gdf_elec_service_areas.merge(
        utility_area_weighted_income[
            [
                "NAME",
                "area_weighted_adjuted_gross_income",
                "total_intersection_area_in_utility",
            ]
        ],
        on="NAME",
        how="left",
    )
    print("\nFinal utility GDF with weighted income:")
    print(gdf_utilities_final.head())


if __name__ == "__main__":
    main()
