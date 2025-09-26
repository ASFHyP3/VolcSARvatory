import asf_search as asf
import cartopy.feature as cfeature
import fsspec
import geopandas as gpd
import pandas as pd
import os
import warnings

from pathlib import Path
from shapely.geometry import Polygon

PARQUET_DIR = Path(__file__).parent / 'parquets'

land_50m = cfeature.NaturalEarthFeature('physical','land','10m')
land_polygons_cartopy = list(land_50m.geometries())
land_gdf = gpd.GeoDataFrame(crs='epsg:4326', geometry=land_polygons_cartopy)

s1_gdf = None

def get_aoi():
    """
    Reads the parquet file with the areas of interest
    """
    aoigdf = gpd.read_parquet(f"{PARQUET_DIR}/aoi_vol.parquet")
    aoigdf = aoigdf.to_crs("EPSG:4326")
    return aoigdf

def add_aoi(id, extent):
    """
    Adds or replaces an area of interest in the parquet file.
    
    Args:
        id: Id for the area of interest.
        extent: List of lon/lat coordinates for the area of interest in the format [minlon, maxlon, minlat, maxlat].
    
    Returns:
        intersection: Geopandas dataframe with the intersection between the area of interest and the land mask. 
    """
    ullon, lrlon, lrlat, ullat = extent
    poly=Polygon([(ullon,ullat,0),(ullon,lrlat,0),(lrlon,lrlat,0),(lrlon,ullat,0)])
    new_aoi = {'name': [id], 'geometry': [poly]}
    new_aoi = gpd.GeoDataFrame(new_aoi, crs="EPSG:4326")
    if os.path.exists(f"{PARQUET_DIR}/aoi_vol.parquet"):
        aoi_gdf = gpd.read_parquet(f"{PARQUET_DIR}/aoi_vol.parquet")
        if len(aoi_gdf[aoi_gdf['name'] == id]) > 0:
            warnings.warn('An AOI with the same ID exists in the dataframe. Replacing...', UserWarning)
            aoi_gdf[aoi_gdf['name'] == id]['geometry'] = poly
        else:
            aoi_gdf = pd.concat([aoi_gdf, new_aoi], ignore_index=True)
    else:
        aoi_gdf = new_aoi
    intersection = gpd.overlay(aoi_gdf, land_gdf, how='intersection')
    intersection.to_parquet(f"{PARQUET_DIR}/aoi_vol.parquet")

    return intersection

def load_s1_gdf():
    """
    Loads a parquet file with the burst ids and the extents.
    """
    s3_url = "s3://its-live-data/autorift_parameters/v001/mission_frames_all.parquet"
    fs = fsspec.filesystem("s3", anon=True)
    gdf = gpd.read_parquet(s3_url, filesystem=fs)
    global s1_gdf
    s1_gdf = gdf[(gdf['mission']=='S1')]

def get_burst_ids(aoi_id = None, aoi_file = None):
    """
    Get the burst ids that intersect the area of interest.
    
    Args:
        aoi_id: Id for the area of interest. If None all the area of interest are taken.
        aoi_file: Path to the parquet file. If None it takes the parquet file in cache.
    
    Returns:
        result: Dictionary where the keys are the burst ids and the area of interests overlapping.
    """
    load_s1_gdf()
    if aoi_file is None:
        aoi_file = f"{PARQUET_DIR}/aoi_vol.parquet"
    aoi_gdf = gpd.read_parquet(aoi_file)
    bursts_gdf = gpd.sjoin(s1_gdf, aoi_gdf, how='inner', predicate='intersects')
    bursts_gdf["area"] = gpd.overlay(s1_gdf, aoi_gdf, how='intersection').area.to_numpy()/bursts_gdf.area.to_numpy()
    result = dict()
    for bid in bursts_gdf["id"].unique():
        asf_res=asf.search(fullBurstID=bid)
        cond=False
        if len(asf_res)>1:
            cond=True
        elif len(asf_res)==1:
            if asf_res[0].properties['stopTime'] is not None:
                cond=True
        if cond:
            if bursts_gdf[bursts_gdf["id"]==bid]["area"].to_numpy()[0] > 0.05:
                if aoi_id is None:
                    result[bid]=bursts_gdf[bursts_gdf["id"]==bid]["name"].unique()
                else:
                    if aoi_id in bursts_gdf[bursts_gdf["id"]==bid]["name"].unique():
                        result[bid]=bursts_gdf[bursts_gdf["id"]==bid]["name"].unique()
    return result
