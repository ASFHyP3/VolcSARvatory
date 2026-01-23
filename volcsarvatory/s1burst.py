import asf_search as asf
import geopandas as gpd
import json
import matplotlib.pyplot as plt
import numpy as np
import pygmt

from datetime import datetime
from pathlib import Path
from volcsarvatory import aoi, pairs
from volcsarvatory import prepare_multibursts as pm

BBOX = json.loads(
    (Path(__file__).parent / 'data' / 'bbox.json').read_text()
)

PARQUET_DIR = Path(__file__).parent / 'data'
#BURST_IDS = json.loads(
#    (Path(__file__).parent / 'data' / 'burst_ids.json').read_text()
#)

def read_aois():
    aoi_ids = [key for key in BBOX]
    if (Path(__file__).parent / 'data' / 'multiburst.json').exists():
        burst_dic = json.loads((Path(__file__).parent / 'data' / 'multiburst.json').read_text())
    else:
        burst_dic = dict()
    modified = False
    for id in aoi_ids:
        added = aoi.add_aoi(id, extent = BBOX[id]["AOI"])
        if not added:
            continue
        else:
            modified = True
            aoigdf = aoi.get_aoi()
        print('AOI', id)
        burst_dict = aoi.get_burst_ids(aoi_id = id)
        burst_ids = [bid for bid in burst_dict.keys()]
        print(len(burst_ids),'bursts')
        multibursts = pm.get_multibursts(burst_ids)
        print(len(multibursts),'multibursts')
        mb_ids=[]
        for multiburst in multibursts:
            dic = multiburst.multiburst_dict
            mb_id = get_mbid(dic)
            burst_dic[mb_id] = dict()
            burst_dic[mb_id]["mb_set"] = dic
            burst_dic[mb_id]["temporal_baseline"] = BBOX[id]["temporal_baseline"]
            burst_dic[mb_id]["season"] = BBOX[id]["season"]
            burst_dic[mb_id]["target_date"] = BBOX[id]["target_date"]
            burst_dic[mb_id]["bridge_years"] = BBOX[id]["bridge_years"]
            mb_ids.append(mb_id)
        print(aoigdf)
        aoigdf.loc[aoigdf['name'] == id,'mb_ids'] = ",".join(mb_ids)
    if modified:
        aoigdf.to_parquet(f"{PARQUET_DIR}/aoi_vol.parquet")
        with open(str(Path(__file__).parent / 'data' / 'multiburst.json'), 'w') as json_file:
            json.dump(burst_dic, json_file)
        update_burst_json()


def get_mbid(dic):
    keys = [key for key in dic.keys()]
    iw1s = [int(key.split('_')[1]) for key in keys if "IW1" in dic[key]]
    iw2s = [int(key.split('_')[1]) for key in keys if "IW2" in dic[key]]
    iw3s = [int(key.split('_')[1]) for key in keys if "IW3" in dic[key]]
    iw1 = "000000"
    iw2 = "000000"
    iw3 = "000000"
    if len(iw1s)>0:
        iw1 = str(min(iw1s)).zfill(6)
    if len(iw2s)>0:
        iw2 = str(min(iw2s)).zfill(6)
    if len(iw3s)>0:
        iw3 = str(min(iw3s)).zfill(6)
    mb_id = keys[0].split('_')[0]+"_"+iw1+"n"+str(len(iw1s)).zfill(2)+"_"+iw2+"n"+str(len(iw2s)).zfill(2)+"_"+iw3+"n"+str(len(iw3s)).zfill(2)

    return mb_id


def update_burst_json():
    burst_dic = json.loads((Path(__file__).parent / 'data' / 'multiburst.json').read_text())
    burst_ids = []
    for mb_id in burst_dic.keys():
        for key in burst_dic[mb_id]["mb_set"]:
            for swath in burst_dic[mb_id]["mb_set"][key]:
                burst_ids.append(f"{key}_{swath}")

    with open(str(Path(__file__).parent / 'data' / 'sentinel1_tiles_to_process.json'), 'w') as json_file:
        json.dump(burst_ids, json_file)


def product_qualifies_for_sentinel1_processing(product: ASFProduct, log_level: int = logging.DEBUG) -> bool:
    """Check if a Sentinel-1 Burst product qualifies for processing."""
    burst_id = product.properties['burst']['fullBurstID']
    if burst_id not in SENTINEL1_BURSTS_TO_PROCESS:
        log.log(log_level, f'{burst_id} disqualifies for processing because it is not from a burst containing land-ice')
        return False

    if (polarization := product.properties['polarization']) not in [asf.constants.VV, asf.constants.HH]:
        log.log(log_level, f'{burst_id} disqualifies for processing because it has a {polarization} polarization')
        return False

    log.log(log_level, f'{burst_id} qualifies for processing')
    return True
