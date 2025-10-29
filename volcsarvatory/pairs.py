import asf_search as asf
import geopandas as gpd
import glob
import opensarlab_lib as osl
import os
import random
import shapely.wkt
import subprocess
from datetime import datetime
from osgeo import gdal, ogr
from pathlib import Path
from shapely.geometry import Polygon
from rasterio.warp import transform_bounds
from tqdm.auto import tqdm
from volcsarvatory import util

def get_coherence(multiburst_dict, num = 1):
    """
    Estimates the mean coherence for random burst(s) pairs in a multiburst set.
    
    Args:
        multiburst_dict: Dictionary where the keys are the burst ids and the elements the swaths.
        num: Number of burst(s) to estimate the mean coherence.
    
    Returns:
        coherence: Dictionary where the keys are the number of days between the pairs and the
                   elements are dictionaries where the keys are the reference dates.
    """
    coherence = dict()
    burst_ids = []
    for bid in multiburst_dict.keys():
        for swath in multiburst_dict[bid]:
            burst_ids.append(bid+'_'+swath)

    bids = [burst_ids[random.randint(0,len(burst_ids)-1)] for i in range(num)]
    for bid in bids:
        prods = asf.search(fullBurstID = bid, start = '2019-12-01', end = '2021-02-01', polarization = asf.POLARIZATION.VV)[::-1]
        for i, ref in enumerate(prods[0:-1]):
            for sec in prods[i+1::]:
                pair = asf.Pair(ref, sec)
                if pair.temporal.days in [6,12,18,24,36,48]:
                    ref_date = ref.properties["stopTime"].split('T')[0]
                    sec_date = sec.properties["stopTime"].split('T')[0]
                    if pair.temporal.days not in coherence.keys():
                        coherence[pair.temporal.days] = dict()
                    else:
                        if ref_date in coherence[pair.temporal.days].keys():
                            coherence[pair.temporal.days][ref_date] += pair.estimate_s1_mean_coherence()/num
                        else:
                            coherence[pair.temporal.days][ref_date] = pair.estimate_s1_mean_coherence()/num
    return coherence

def prepare_multiburst_jobs(refs, secs, project_name, hyp3, looks = '20x4', apply_water_mask = True):
    """
    Prepares the multiburst jobs from the pairs returned by an SBAS network.
    
    Args:
        refs: Reference scene ids.
        secs: Secondary scene ids.
        hyp3: Instance of HyP3 where the user has been logged in.
        project_name: Name of the project in HyP3.
        looks: Multilooking in the final products.
        apply_water_mask: If true it applies a water mask in the HyP3 processing.
        
    Returns:
        insar_jobs: List with prepared jobs for HyP3
    """
    insar_jobs = []
    bursts=[ref[0:13] for ref in refs]
    ubursts=list(set(bursts))
    lenburst=int(len(refs)/len(ubursts))

    for i in range(lenburst):
        ref=[refs[i+j*lenburst] for j in range(len(ubursts))]
        sec=[secs[i+j*lenburst] for j in range(len(ubursts))]
        insar_jobs.append(hyp3.prepare_insar_isce_multi_burst_job(ref, sec, name=project_name, apply_water_mask=True))
    return insar_jobs

def submit_jobs(insar_jobs, hyp3):
    """
    Submits prepared multiburst jobs.
    
    Args:
        insar_jobs: Prepared multiburst jobs.
        hyp3: Instance of HyP3 where the user has been logged in.
    
    Returns:
        jobs: List of submitted batches.
    """
    batches = int(len(insar_jobs)/100)+1
    jobs = []
    for batch in range(batches):
        ini=batch*100
        if batch==batches-1:
            fin=batch*100+len(insar_jobs)%100
        else:
            fin=(batch+1)*100
        jobs.append(hyp3.submit_prepared_jobs(insar_jobs[ini:fin]))
    return jobs
    
def download_pairs(project_name, hyp3, folder = None):
    """
    Downloads HyP3 products and renames files to meet MintPy standards
    
    Args:
        project_name: Name of the HyP3 project.
        hyp3: Instance of HyP3 where the user has been logged in.
        folder: Folder name that will contain the downloaded products. If None it will create a folder with the project name.
    """
    jobs = hyp3.find_jobs(name=project_name)

    cwd = os.getcwd()
    if folder is None:
        folder = project_name
    if not os.path.isdir(folder):
        os.mkdir(folder)
    folder = Path(folder)
    file_list = jobs.download_files(folder)
    for z in file_list:
        osl.asf_unzip(str(folder), str(z))
        z.unlink()

    os.chdir(str(folder))
    folders=glob.glob('./*')
    folders=[fol for fol in folders if os.path.isdir(fol)]

    for fol in folders:
        new = True
        if str(fol).count('_') > 7:
            new = False
        os.chdir(fol)
        fs=glob.glob('./*')
        txts=[t for t in fs if '.txt' in t and 'README' not in t]
        ar=open(txts[0])
        lines=ar.readlines()
        ar.close()
        burst=lines[0].split('_')[1]+'_'+lines[0].split('_')[2]
        for f in fs:
            name=os.path.basename(f)
            if new:
                newname = 'S1_' + burst + '_' + '_'.join([n for n in name.split('_')[3:]])
            else:
                newname = 'S1_' + burst + '_' + '_'.join([n for n in name.split('_')[10:]])
            if '.txt' in newname and 'README' not in newname:
                foldername=newname.split('.')[0]
            subprocess.call('mv '+name+' '+newname,shell=True)
        os.chdir(cwd)
        os.chdir(str(folder))
        subprocess.call('mv '+os.path.basename(fol)+' '+foldername,shell=True)
    os.chdir(cwd)
    
def set_same_frame(folder, wgs84 = False):
    """
    Checks the coordinate system for all the files in the folder and reprojects them if necessary
    
    Args:
        folder: Path to the folder that has the HyP3 products.
        wgs84: If True reprojects all the files to WGS84 system.
    """
    data_path = Path(folder)
    dem = sorted(list(data_path.glob('*/*dem*.tif')))
    lv_phi = sorted(list(data_path.glob('*/*lv_phi*.tif')))
    lv_theta = sorted(list(data_path.glob('*/*lv_theta*.tif')))
    water_mask = sorted(list(data_path.glob('*/*_water_mask*.tif')))
    unw = sorted(list(data_path.glob('*/*_unw_phase*.tif')))
    corr = sorted(list(data_path.glob('*/*_corr*.tif')))
    conn_comp = sorted(list(data_path.glob('*/*_conncomp*.tif')))
    tiff_path = dem + lv_phi + lv_theta + water_mask + unw + corr + conn_comp
    
    gdf = gpd.GeoDataFrame(
        {
        'tiff_path': tiff_path,
        'EPSG': [util.get_epsg(p) for p in tiff_path],
        'geometry': [util.get_geotiff_bbox(p) for p in tiff_path],
        }
    )

    # check for multiple projections and project to the predominant EPSG 
    if gdf['EPSG'].nunique() > 1:
        proj_count = gdf['EPSG'].value_counts()
        predominant_epsg = proj_count.idxmax()
        print(f'reprojecting to predominant EPSG: {predominant_epsg}')
        for _, row in gdf.loc[gdf['EPSG'] != predominant_epsg].iterrows():
            pth = row['tiff_path']
            no_data_val = util.get_no_data_val(pth)
            res = util.get_res(pth)
        
            temp = pth.parent/f"temp_{pth.stem}.tif"
            pth.rename(temp)
            src_epsg = row['EPSG']

            warp_options = {
                "dstSRS":f"EPSG:{predominant_epsg}", "srcSRS":f"EPSG:{src_epsg}",
                "targetAlignedPixels":True,
                "xRes":res, "yRes":res,
                "dstNodata": no_data_val
            }
            gdal.Warp(str(pth), str(temp), **warp_options)
            temp.unlink()

        gdf = gpd.GeoDataFrame(
        {
        'tiff_path': tiff_path,
        'EPSG': [util.get_epsg(p) for p in tiff_path],
        'geometry': [util.get_geotiff_bbox(p) for p in tiff_path],
        }
        )
    common_extents = osl.get_common_coverage_extents(unw)
    xmin, ymin, xmax, ymax = transform_bounds(int(osl.get_projection(str(unw[0]))), 3857, *common_extents)
    common_extents_3857 = [xmin, ymin, xmax, ymax]
    print(common_extents)
    correct_wkt_input = False
    while not correct_wkt_input:
        epsg = int(gdf.iloc[0]['EPSG'])
        wkt = (f'POLYGON(({common_extents[0]} {common_extents[1]}, {common_extents[2]} {common_extents[1]}, {common_extents[2]} '
               f'{common_extents[3]}, {common_extents[0]} {common_extents[3]}, {common_extents[0]} {common_extents[1]}))')
        print(wkt)
        wkt_shapely_geom = shapely.wkt.loads(wkt)
        wkt_ogr_geom = ogr.CreateGeometryFromWkt(wkt)
        if not util.check_within_bounds(wkt_shapely_geom, gdf):
            print('WKT exceeds bounds of at least one dataset')
            raise Exception('Error determining area of common coverage')

        correct_wkt_input = True

    shp_path = data_path / f'shape_{datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")}.shp'
    util.save_shapefile(wkt_ogr_geom, epsg, shp_path)
    for pth in tqdm(gdf['tiff_path']):
        print(f'Subsetting: {pth}')
        temp_pth = pth.parent/f'subset_{pth.name}'
        gdal.Translate(destName=str(temp_pth), srcDS=str(pth), projWin=[common_extents[0], common_extents[3], common_extents[2], common_extents[1]])
        pth.unlink()
        temp_pth.rename(pth)

    if wgs84:
        for pth in tqdm(gdf['tiff_path']):
            print(f'Converting {pth} to WGS84')
            gdal.Warp(str(pth), str(pth), dstSRS='EPSG:4326')
