import h5py
import numpy as np
import matplotlib.pyplot as plt
import shutil
import xarray as xr

from scipy.interpolate import RegularGridInterpolator


def change_reference(h5file, ref_coords):
    """Changes the reference pixel on the timeseries.

    Args:
        h5file: H5 file with the timeseries.
        ref_coords: reference pixel in lon/lat coordinates.
    """
    h5f = h5py.File(h5file,'r+')
    dates = [date.decode('utf-8') for date in h5f['date'][:]]
    timeseries = h5f['timeseries'][:]
    ul = (float(h5f.attrs['X_FIRST']), float(h5f.attrs['Y_FIRST']))
    steps = (float(h5f.attrs['X_STEP']), float(h5f.attrs['Y_STEP']))
    lons = np.linspace(ul[0], ul[0]+steps[0]*timeseries.shape[2], timeseries.shape[2])
    lats = np.linspace(ul[1]+steps[1]*timeseries.shape[1], ul[1], timeseries.shape[1])[::-1]
    j=np.argmin(np.abs(lons-ref_coords[0]))
    i=np.argmin(np.abs(lats-ref_coords[1]))
    timeseries[timeseries==0]=np.nan
    for t in range(timeseries.shape[0]):
        timeseries[t,:,:] -= timeseries[t,i,j]
    timeseries[np.isnan(timeseries)]=0
    timeseries -= timeseries[0,:,:]
    h5f['timeseries'][:] = timeseries
    h5f.attrs['REF_DATE'] = dates[0]
    h5f.attrs['REF_LAT'] = lats[i]
    h5f.attrs['REF_LON'] = lons[j]
    h5f.attrs['REF_X'] = j
    h5f.attrs['REF_Y'] = i
    h5f.close()


def merge_timeseries(reference, h5file):
    """Merges two timeseries.

    Args:
        reference: H5 file with the first timeseries.
        h5file: H5 file with the second timeseries.
    """
    h5f = h5py.File(reference)
    dates1 = [date.decode('utf-8') for date in h5f['date'][:]]
    timeseries1 = h5f['timeseries'][:]
    ref_pix = (h5f.attrs['REF_Y'], h5f.attrs['REF_X'])
    ref_coord = (h5f.attrs['REF_LON'], h5f.attrs['REF_LAT'])
    ul = (float(h5f.attrs['X_FIRST']), float(h5f.attrs['Y_FIRST']))
    steps = (float(h5f.attrs['X_STEP']), float(h5f.attrs['Y_STEP']))
    lons = np.linspace(ul[0], ul[0]+steps[0]*timeseries1.shape[2], timeseries1.shape[2])
    lats = np.linspace(ul[1]+steps[1]*timeseries1.shape[1], ul[1], timeseries1.shape[1])
    LONS,LATS=np.meshgrid(lons,lats)
    shape = (timeseries1.shape[1], timeseries1.shape[2])
    h5f.close()

    h5f = h5py.File(h5file)
    dates2 = [date.decode('utf-8') for date in h5f['date'][:]]
    timeseries = h5f['timeseries'][:]
    ul_sec = (float(h5f.attrs['X_FIRST']), float(h5f.attrs['Y_FIRST']))
    steps_sec = (float(h5f.attrs['X_STEP']), float(h5f.attrs['Y_STEP']))
    lons_sec = np.linspace(ul_sec[0], ul_sec[0]+steps_sec[0]*timeseries.shape[2], timeseries.shape[2])
    lats_sec = np.linspace(ul_sec[1]+steps_sec[1]*timeseries.shape[1], ul_sec[1], timeseries.shape[1])
    shape_sec = (timeseries.shape[1], timeseries.shape[2])
    h5f.close()
    
    
    newdates = list(dates1)+sorted(list((set(dates2)-set(dates1))))
    intdates = set(dates1).intersection(set(dates2))
    index1 = dates1.index(sorted(intdates)[int(len(intdates)/2)])
    index2 = dates2.index(sorted(intdates)[int(len(intdates)/2)])
    index3 = dates2.index(sorted(intdates)[-1])
    print(index1,index2)
    newtimeseries = np.ones((timeseries.shape[0],timeseries1.shape[1], timeseries1.shape[2]))*np.nan
    for i in range(timeseries.shape[0]):
        interp_sec = RegularGridInterpolator((lats_sec,lons_sec), timeseries[i,:,:], method='nearest', bounds_error=False, fill_value=0.0)
        newtimeslice = interp_sec(np.c_[LATS.ravel(),LONS.ravel()])
        newtimeslice = newtimeslice.reshape((LONS.shape[0], LONS.shape[1]))
        newtimeslice[newtimeslice==0] = np.nan
        newtimeslice -= newtimeslice[int(ref_pix[0]),int(ref_pix[1])]
        newtimeslice[np.isnan(newtimeslice)] = 0
        newtimeseries[i,:,:] = newtimeslice

    timeseries1 -= timeseries1[index1,:,:]
    newtimeseries -= newtimeseries[index2,:,:]
    timeseries_all = np.ones((len(newdates),timeseries1.shape[1], timeseries1.shape[2]))
    timeseries_all[0:timeseries1.shape[0],:,:] = timeseries1
    timeseries_all[timeseries1.shape[0]:,:,:] = newtimeseries[index3+1:,:,:]
    timeseries_all -= timeseries_all[0,:,:]
    
    shutil.copy(reference,'newtimeseries.h5')
    h5f = h5py.File('newtimeseries.h5','r+')
    del h5f['date']
    h5f.create_dataset('date', data=np.array(newdates, dtype="S8"))
    h5f.attrs['REF_DATE'] = dates1[0]
    h5f.attrs['END_DATE'] = dates2[-1]
    del h5f['timeseries']
    h5f.create_dataset('timeseries', data=timeseries_all)
    h5f.close()

