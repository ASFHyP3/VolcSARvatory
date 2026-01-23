import asf_search as asf
from datetime import datetime, timedelta
from volcsarvatory import pairs
from volcsarvatory import prepare_multibursts as pm

def first_date_burst(burst_id):
    results = asf.search(fullBurstID=burst_id)
    dates = sorted([r.properties['stopTime'] for r in results if r.properties['stopTime'] is not None])
    return dates[0].split('T')[0]


def first_date_multiburst(dic):
    keys = [key for key in dic.keys()]
    burst_id = keys[0]+dic[keys[0]][0]

    return first_date_burst(burst_id)


def get_season(dic):
    coherence = pairs.get_coherence(dic, num = 1)
    keys=[key for key in coherence.keys()]
    start=datetime.now()
    end=datetime.strptime("2014-01-01", '%Y-%m-%d')
    bridges=[]
    for days in keys:
        #days = 12
        cohs = [coherence[days][ref] for ref in coherence[days].keys()]
        mincoh = min(cohs)
        maxcoh = max(cohs)
        dates = [datetime.strptime(ref, '%Y-%m-%d') for ref in coherence[days].keys()]
        newdates = [datetime.strptime(ref, '%Y-%m-%d') for ref in coherence[days].keys() if not coherence[days][ref]==mincoh]
        maxdates = sorted([datetime.strptime(ref, '%Y-%m-%d') for ref in coherence[days].keys() if coherence[days][ref]==maxcoh])
        meandate = maxdates[0]+timedelta(days=int((maxdates[-1]-maxdates[0]).days/2))
        diffdays = [abs((date-meandate).days) for date in maxdates]
        bridges.append(maxdates[diffdays.index(min(diffdays))])
        if start>min(newdates):
            start=min(newdates)
        if end<max(newdates):
            end=max(newdates)

    bridge = sorted(bridges)[int(len(bridges)/2)-1]
    season = (start, end)

    return season, bridge


def get_sbas(dic):
    start = first_date_multiburst(dic)
    season, bridge = get_season(dic)
    multiburst = asf.Multiburst(dic)
    opts = asf.ASFSearchOptions(
        **{
            "start": start,
            "end": datetime.now().strftime("%Y-%m-%d"),
            "season": pm.get_julian_season(season)
      }
    )

    network = asf.Network(
        multiburst = multiburst,
        perp_baseline=800, 
        inseason_temporal_baseline=48,
        bridge_target_date=bridge.strftime("%m-%d"),
        bridge_year_threshold=1,
        opts=opts)

    network.connect_components()

    refs, secs = network.get_multi_burst_pair_ids()

    season = ("1-1", "12-31")
    opts = asf.ASFSearchOptions(
        **{
            "start": datetime.now().strftime("%Y-%m-%d")-timedelta(days = 365),
            "end": datetime.now().strftime("%Y-%m-%d"),
            "season": pm.get_julian_season(season)
      }
    )

    network = asf.Network(
        multiburst = multiburst,
        perp_baseline=800, 
        inseason_temporal_baseline=144,
        bridge_target_date=datetime.now().strftime("%m-%d"),
        bridge_year_threshold=1,
        opts=opts)

    network.connect_components()

    refs_add, secs_add = network.get_multi_burst_pair_ids()

    refs += refs_add
    secs += secs_add

    return refs, secs
