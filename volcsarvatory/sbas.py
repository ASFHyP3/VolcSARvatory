import asf_search as asf
from volcsarvatory import pairs

def get_first_date(burst_id):
    results = asf.search(fullBurstID=burst_id)
    dates = sorted([r.properties['stopTime'] for r in results if r.properties['stopTime'] is not None])
    return dates[0]


def get_first_date_multiburst(dic):
    keys = [key for key in dic.keys()]
    burst_id = keys[0]+dic[keys[0]][0]

    return burst_id
    

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
