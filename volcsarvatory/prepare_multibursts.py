import asf_search as asf
import pandas as pd
import time
from datetime import datetime
from asf_search.exceptions import InvalidMultiBurstCountError, InvalidMultiBurstTopologyError

def get_julian_season(season) -> tuple[int,int]:
    """
    Assigns a year to a season.
    
    Args:
        season: Tuple with the initial and final month-day.
    
    Returns:
        season_dates: Tuple with datetimes that represent the season.  
    """
    season_start_ts = pd.Timestamp(
        datetime.strptime(f"{season[0]}-0001", "%m-%d-%Y"), tz="UTC"
        )
    season_start_day = season_start_ts.timetuple().tm_yday
    season_end_ts = pd.Timestamp(
        datetime.strptime(f"{season[1]}-0001", "%m-%d-%Y"), tz="UTC"
    )
    season_end_day = season_end_ts.timetuple().tm_yday
    return (season_start_day, season_end_day)

def get_multibursts(burst_ids):
    """
    Get Multiburst objects from a list of burst ids.
    
    Args:
        burst_ids: List with the burst IDs.
    
    Returns:
        multibursts: List of Multiburst objects associated with the burst IDs.
    """
    burst_ids = list(set(sorted(burst_ids)))
    path_dict = dict()
    for bid in burst_ids:
        path = bid[0:3]
        if path not in path_dict.keys():
            path_dict[path] = [bid]
        else:
            path_dict[path].append(bid)

    multibursts = []
    for path in path_dict.keys():
        multibursts += get_multibursts_path(path_dict[path])

    return multibursts

def get_multibursts_path(burst_ids):
    """
    Get Multiburst objects from a list of burst ids from one path.
    
    Args:
        burst_ids: List with the burst IDs associated with a path.
    
    Returns:
        multibursts: List of Multiburst objects associated with the burst IDs.
    """
    multiburst_dict = dict()
    for bid in burst_ids:
        path = bid[0:3]
        id = bid[0:-4]
        swath = bid[-3::]
        if not id in multiburst_dict.keys():
            multiburst_dict[id] = (swath,)
        else:
            multiburst_dict[id] = tuple(sorted(multiburst_dict[id] + (swath,)))

    try:
        multiburst = get_multiburst(multiburst_dict)
        return [multiburst]
    except InvalidMultiBurstCountError:
        cont = 0
        multiburst_dicts = []
        multiburst_set = dict()
        for bid in sorted(multiburst_dict.keys()):
            if (cont + len(multiburst_dict[bid])) > 30:
                multiburst_dicts.append(multiburst_set)
                multiburst_set = dict()
                cont = 0
            multiburst_set[bid] = multiburst_dict[bid]
            cont += len(multiburst_dict[bid])
        if cont > 0:
            multiburst_dicts.append(multiburst_set)
    except InvalidMultiBurstTopologyError:
        multiburst_dicts = [multiburst_dict]
    multibursts = []
    for multiburst_dict in multiburst_dicts:
        try:
            multiburst = get_multiburst(multiburst_dict)
            multibursts.append(multiburst)
        except InvalidMultiBurstTopologyError:
            multibursts_sets = split_multiburst(multiburst_dict)
            for multiburst_dict in multibursts_sets:
                multibursts.append(get_multiburst(multiburst_dict))

    return multibursts

def get_multiburst(multiburst_dict):
    try:
        multiburst = asf.MultiBurst(multiburst_dict)
    except InvalidMultiBurstTopologyError as e:
        raise(e)
    except InvalidMultiBurstCountError as e:
        raise(e)
    except ConnectionError:
        time.sleep(5)
        multiburst = asf.MultiBurst(multiburst_dict)
    except ConnectionResetError:
        time.sleep(5)
        multiburst = asf.MultiBurst(multiburst_dict)
    except OSError as e:
        time.sleep(5)
        multiburst = asf.MultiBurst(multiburst_dict)
    return multiburst

def split_count(multiburst_dict):
    """
    Splits a multiburst set in case it is over 30 bursts.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        multiburst_dicts: List of the splitted dictionary.
    """
    cont = 0
    multiburst_dicts = []
    multiburst_set = dict()
    for bid in sorted(multiburst_dict.keys()):
        if (cont + len(multiburst_dict[bid])) > 30:
            multiburst_dicts.append(multiburst_set)
            multiburst_set = dict()
            cont = 0
        multiburst_set[bid] = multiburst_dict[bid]
        cont += len(multiburst_dict[bid])
    if cont > 0:
        multiburst_dicts.append(multiburst_set)

    return multiburst_dicts

def split_multiburst(multiburst_dict):
    """
    Splits a multiburst horizontally or vertically and fill holes if it doesn't meet burst2safe standards.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        new_sets: List of the splitted dictionary.
    """
    new_dicts = split_vertical_multiburst(multiburst_dict)
    new_sets = []
    for new_dict in new_dicts:
        new_dict = fill_holes(new_dict)
        new_set = complete_sides(new_dict)
        for new_dict in new_set:
            new_sets += split_horizontal_multiburst(new_dict)

    return new_sets

def split_vertical_multiburst(multiburst_dict):
    """
    Splits a multiburst vertically if it finds a vertical gap in the set.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        new_sets: List of the splitted dictionary.
    """
    ids = [bid for bid in sorted(multiburst_dict.keys())]
    id_sets=[]
    previous = 0
    for i,id in enumerate(ids[0:-1]):
        current = int(id.split('_')[1])
        next = int(ids[i+1].split('_')[1])
        if not current == (next-1):
            id_sets.append(ids[previous:(i+1)])
            previous = i+1
    id_sets.append(ids[previous::])
    new_sets=[]
    for id_set in id_sets:
        new_dict=dict()
        for bid in id_set:
            new_dict[bid] = multiburst_dict[bid]
        new_sets.append(new_dict)
    return new_sets

def fill_holes(multiburst_dict):
    """
    Fills a multiburst set when a hole is found in the set.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        multiburst_dict: Dictionary with complemented set.
    """
    for bid in multiburst_dict.keys():
        if "IW1" in multiburst_dict[bid] and "IW3" in multiburst_dict[bid] and not "IW2" in multiburst_dict[bid]:
            multiburst_dict[bid] = tuple(sorted(multiburst_dict[bid] + ("IW2",)))
    ranges = dict()
    for swath in ["IW1","IW2","IW3"]:
        ids = sorted(list(set([bid for bid in multiburst_dict.keys() if swath in multiburst_dict[bid]])))
        if len(ids) > 0:
            ranges[swath] = (int(ids[0].split('_')[1]), int(ids[-1].split('_')[1]))
            dif = abs(int(ids[0].split('_')[1])-int(ids[-1].split('_')[1]))
            if not dif == (len(ids)-1):
                for i,id in enumerate(ids[0:-1]):
                    current = int(id.split('_')[1])
                    next = int(ids[i+1].split('_')[1])
                    if not current == next-1:
                        for j in range(current+1, next):
                            multiburst_dict[id[0:4]+str(j)] = tuple(sorted(multiburst_dict[id[0:4]+str(j)] +(swath,)))
    return multiburst_dict


def get_ranges(multiburst_dict):
    """
    Finds the ranges for each swath in a multiburst set.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        ranges: Tuple with the initial and final burst.
        ids: List of burst IDs in the multiburst set.
    """
    ranges = dict()
    ids = dict()
    swaths = ["IW1","IW2","IW3"]
    for swath in swaths:
        ids[swath] = sorted(list(set([bid for bid in multiburst_dict.keys() if swath in multiburst_dict[bid]])))
        if len(ids[swath]) > 0:
            ranges[swath] = (int(ids[swath][0].split('_')[1]), int(ids[swath][-1].split('_')[1]))
    return ranges, ids

def complete_sides(multiburst_dict):
    """
    Adds bursts to a multiburst set in case the ranges between adjacent swaths is more than 1 and less than 3.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        multiburst_dicts: List of complemented multiburst sets.
    """
    swaths = ["IW1","IW2","IW3"]
    for i in range(3):
        ranges, ids = get_ranges(multiburst_dict)
        if i == 2:
           i = 0
        current = swaths[i]
        next = swaths[i+1]
        if not current in ranges.keys() or not next in ranges.keys():
            continue
        path = ids[current][0][0:3]
        split = abs(ranges[current][0]-ranges[next][0]) > 3 or abs(ranges[current][1]-ranges[next][1]) > 3
        valid = abs(ranges[current][0]-ranges[next][0]) <= 1 and abs(ranges[current][1]-ranges[next][1]) <= 1 
        if split or valid:
            continue
        else:
            if abs(ranges[current][0]-ranges[next][0]) > 1:
                if ranges[current][0] > ranges[next][0]:
                    for id in range(ranges[next][0]+1, ranges[current][0]):
                        bid = path+'_'+str(id).zfill(6)
                        multiburst_dict[bid] = tuple(sorted(multiburst_dict[bid] +(current,)))
                else:
                    for id in range(ranges[current][0]+1, ranges[next][0]):
                        bid = path+'_'+str(id).zfill(6)
                        multiburst_dict[bid] = tuple(sorted(multiburst_dict[bid] +(next,)))
            if abs(ranges[current][1]-ranges[next][1]) > 1:
                if ranges[current][1] > ranges[next][1]:
                    for id in range(ranges[next][1]+1, ranges[current][1]):                
                        bid = path+'_'+str(id).zfill(6)
                        multiburst_dict[bid] = tuple(sorted(multiburst_dict[bid] +(next,)))
                else:
                    for id in range(ranges[current][1]+1, ranges[next][1]):
                        bid = path+'_'+str(id).zfill(6)
                        multiburst_dict[bid] = tuple(sorted(multiburst_dict[bid] +(current,)))
    multiburst_dicts = split_count(multiburst_dict)
    return multiburst_dicts

def split_horizontal_multiburst(multiburst_dict):
    """
    Splits a multiburst set horizontally in case the difference between adjacent ranges swaths is more than 1.
    
    Args:
        multiburst_dict: Dictionary where the keys are burst IDs and the elements are the swaths.
    
    Returns:
        multiburst_dicts: List with the splitted multiburst set.
    """
    ranges, ids = get_ranges(multiburst_dict)
    
    if not "IW1" in ranges.keys() or not "IW2" in ranges.keys():
        split12 = False
    else:
        split12 = abs(ranges["IW1"][0]-ranges["IW2"][0]) > 1 or abs(ranges["IW1"][1]-ranges["IW2"][1]) > 1    
    if not "IW2" in ranges.keys() or not "IW3" in ranges.keys():
        split23 = False
    else:
        split23 = abs(ranges["IW2"][0]-ranges["IW3"][0]) > 1 or abs(ranges["IW2"][1]-ranges["IW3"][1]) > 1
    if split12 and split23:
        iw1 = dict()
        iw2 = dict()
        iw3 = dict()
        for bid in multiburst_dict.keys():
            swaths = multiburst_dict[bid]
            if "IW1" in swaths:
                iw1[bid] = ("IW1",)
            if "IW2" in swaths:
                iw2[bid] = ("IW2",)
            if "IW3" in swaths:
                iw3[bid] = ("IW3",)
        return [iw1, iw2, iw3]
    elif split12:
        iw1 = dict()
        rest = dict()
        for bid in multiburst_dict.keys():
            swaths = multiburst_dict[bid]
            if "IW1" in swaths:
                iw1[bid] = ("IW1",)
            if "IW2" in swaths:
                if bid in rest.keys():
                    rest[bid] = tuple(sorted(rest[bid] + ("IW2",)))
                else:
                    rest[bid] = ("IW2",)
            if "IW3" in swaths:
                if bid in rest.keys():
                    rest[bid] = tuple(sorted(rest[bid] + ("IW3",)))
                else:
                    rest[bid] = ("IW3",)
        return [iw1, rest]
    elif split23:
        iw3 = dict()
        rest = dict()
        for bid in multiburst_dict.keys():
            swaths = multiburst_dict[bid]
            if "IW3" in swaths:
                iw3[bid] = ("IW3",)
            if "IW1" in swaths:
                if bid in rest.keys():
                    rest[bid] = tuple(sorted(rest[bid] + ("IW1",)))
                else:
                    rest[bid] = ("IW1",)
            if "IW2" in swaths:
                if bid in rest.keys():
                    rest[bid] = tuple(sorted(rest[bid] + ("IW2",)))
                else:
                    rest[bid] = ("IW2",)
        return [rest, iw3]
    else:
        return [multiburst_dict]

