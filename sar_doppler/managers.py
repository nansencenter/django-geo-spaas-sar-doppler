import os, warnings
from math import sin, pi, cos, acos, copysign
import numpy as np
from scipy.ndimage.filters import median_filter

from dateutil.parser import parse
from datetime import timedelta

from django.conf import settings
from django.utils import timezone
from django.db import models
from django.contrib.gis.geos import WKTReader

from geospaas.utils import nansat_filename, media_path, product_path
from geospaas.vocabularies.models import Parameter
from geospaas.catalog.models import DatasetParameter, GeographicLocation
from geospaas.catalog.models import Dataset, DatasetURI
from geospaas.viewer.models import Visualization
from geospaas.viewer.models import VisualizationParameter
from geospaas.nansat_ingestor.managers import DatasetManager as DM

from nansat.nansat import Nansat
from nansat.nsr import NSR
from nansat.domain import Domain
from nansat.figure import Figure
from sardoppler.sardoppler import Doppler


class DatasetManager(DM):

    def get_or_create(self, uri, reprocess=False, *args, **kwargs):
        # ingest file to db
        ds, created = super(DatasetManager, self).get_or_create(uri, *args,
                **kwargs)
        if not type(ds)==Dataset:
            return ds, False

        # set Dataset entry_title
        ds.entry_title = 'SAR Doppler'
        ds.save()

        fn = nansat_filename(uri)
        n = Nansat(fn, subswath=0)
        gg = WKTReader().read(n.get_border_wkt())
        if ds.geographic_location.geometry.area>gg.area and not reprocess:
            return ds, False

        ''' Update dataset border geometry

        This must be done every time a Doppler file is processed. It is time
        consuming but apparently the only way to do it. Could be checked
        though...
        '''
        n_subswaths = 5
        swath_data = {}
        lon = {}
        lat = {}
        astep = {}
        rstep = {}
        az_left_lon = {}
        ra_upper_lon = {}
        az_right_lon = {}
        ra_lower_lon = {}
        az_left_lat = {}
        ra_upper_lat = {}
        az_right_lat = {}
        ra_lower_lat = {}
        num_border_points = 10
        border = 'POLYGON(('
        for i in range(n_subswaths):
            # Read subswaths 
            swath_data[i] = Doppler(fn, subswath=i)
            # Should use nansat.domain.get_border - see nansat issue #166
            # (https://github.com/nansencenter/nansat/issues/166)
            lon[i], lat[i] = swath_data[i].get_geolocation_grids()
            astep[i] = max(1, (lon[i].shape[0]/2*2-1) / num_border_points)
            rstep[i] = max(1, (lon[i].shape[1]/2*2-1) / num_border_points)
            az_left_lon[i] = lon[i][0:-1:astep[i],0]
            az_left_lat[i] = lat[i][0:-1:astep[i],0]
            az_right_lon[i] = lon[i][0:-1:astep[i],-1]
            az_right_lat[i] = lat[i][0:-1:astep[i],-1]
            ra_upper_lon[i] = lon[i][-1,0:-1:rstep[i]] 
            ra_upper_lat[i] = lat[i][-1,0:-1:rstep[i]]
            ra_lower_lon[i] = lon[i][0,0:-1:rstep[i]]
            ra_lower_lat[i] = lat[i][0,0:-1:rstep[i]] 
        lons = np.concatenate((az_left_lon[0], ra_upper_lon[0],
            ra_upper_lon[1], ra_upper_lon[2], ra_upper_lon[3],
            ra_upper_lon[4], np.flipud(az_right_lon[4]),
            np.flipud(ra_lower_lon[4]),
            np.flipud(ra_lower_lon[3]), np.flipud(ra_lower_lon[2]),
            np.flipud(ra_lower_lon[1]),
            np.flipud(ra_lower_lon[0])))
        # apply 180 degree correction to longitude - code copied from
        # get_border_wkt...
        for ilon, llo in enumerate(lons):
            lons[ilon] = copysign(acos(cos(llo * pi / 180.)) / pi * 180,
                    sin(llo * pi / 180.))
        lats = np.concatenate((az_left_lat[0], ra_upper_lat[0],
            ra_upper_lat[1], ra_upper_lat[2], ra_upper_lat[3],
            ra_upper_lat[4], np.flipud(az_right_lat[4]),
            np.flipud(ra_lower_lat[4]),
            np.flipud(ra_lower_lat[3]), np.flipud(ra_lower_lat[2]),
            np.flipud(ra_lower_lat[1]),
            np.flipud(ra_lower_lat[0])))
        polyCont = ','.join(str(llo) + ' ' + str(lla) for llo, lla in zip(lons,
            lats))
        wkt = 'POLYGON((%s))' % polyCont
        new_geometry = WKTReader().read(wkt)

        # Get geolocation of dataset - this must be updated
        geoloc = ds.geographic_location
        # Check geometry, return if it is the same as the stored one
        if geoloc.geometry == new_geometry and not reprocess:
            return ds, False

        if geoloc.geometry != new_geometry:
            # Change the dataset geolocation to cover all subswaths
            geoloc.geometry = new_geometry
            geoloc.save()

        ''' Create data products
        '''
        mm = self.__module__.split('.')
        module = '%s.%s' %(mm[0],mm[1])
        mp = media_path(module, swath_data[i].filename)
        ppath = product_path(module, swath_data[i].filename)

        for i in range(n_subswaths):
            is_corrupted = False
            # Check if the file is corrupted
            try:
                inci = swath_data[i]['incidence_angle']
            except:
                is_corrupted = True
                continue
            # Add Doppler anomaly
            swath_data[i].add_band(array=swath_data[i].anomaly(), parameters={
                'wkv':
                'anomaly_of_surface_backwards_doppler_centroid_frequency_shift_of_radar_wave'
            })
            # Find matching NCEP forecast wind field
            wind = Dataset.objects.filter(
                    source__platform__short_name = 'NCEP-GFS', 
                    time_coverage_start__range = [
                        parse(swath_data[i].get_metadata()['time_coverage_start'])
                        - timedelta(hours=3),
                        parse(swath_data[i].get_metadata()['time_coverage_start'])
                        + timedelta(hours=3)
                    ]
                )
            bandnum = swath_data[i]._get_band_number({
                'standard_name': \
                    'surface_backwards_doppler_centroid_frequency_shift_of_radar_wave',
                })
            pol = swath_data[i].get_metadata(bandID=bandnum, key='polarization')
            wavenumber = 5331004416. / 299792458. * 2 * np.pi       # ASAR
            
            #####
            wind = None         # wind correction should NOT be applied for sea ice.
            #####
            
            if wind:
                dates = [w.time_coverage_start for w in wind]
                nearest_date = min(dates, key=lambda d:
                        abs(d-parse(swath_data[i].get_metadata()['time_coverage_start']).replace(tzinfo=timezone.utc)))
                fww = swath_data[i].wind_waves_doppler(
                        nansat_filename(wind[dates.index(nearest_date)].dataseturi_set.all()[0].uri),
                        pol
                    )
                swath_data[i].add_band(array=fww, parameters={
                    'wkv':
                    'surface_backwards_doppler_frequency_shift_of_radar_wave_due_to_wind_waves'
                })
                fdg = swath_data[i].geophysical_doppler_shift(
                    wind=nansat_filename(wind[dates.index(nearest_date)].dataseturi_set.all()[0].uri))

                # Estimate current by subtracting wind-waves Doppler
                theta = swath_data[i]['incidence_angle']*np.pi/180.
                vcurrent = -np.pi*(fdg - fww)/(wavenumber * np.sin(theta))
                ## Smooth...
                #vcurrent = median_filter(vcurrent, size=(3,3))
                swath_data[i].add_band(array=vcurrent,
                        parameters={'name':'raw_Ur'})
            else:
                fww=None
                fdg = swath_data[i].geophysical_doppler_shift()
                # Estimate current by subtracting wind-waves Doppler
                theta = swath_data[i]['incidence_angle']*np.pi/180.
                vcurrent = -np.pi*(fdg)/(wavenumber * np.sin(theta))
                vcurrent[swath_data[i]['valid_doppler']!=2] = np.nan
                ## Smooth...
                #vcurrent = median_filter(vcurrent, size=(3,3))
                swath_data[i].add_band(array=vcurrent,
                        parameters={'name':'raw_Ur'})

            swath_data[i].add_band(array=fdg,
                parameters={'name':'raw_fdg'}
            )
            # Add antenna pattern corrected sigma0
            swath_data[i].add_band(array=swath_data[i].corrected_sigma0(), parameters={
                'name': 'corrected_sigma0_%s' % pol
            })


        # calculate dc bias using land pixels from all subswaths
        buff = 3
        land_fdg = []
        for i in range(n_subswaths):
            landmask = np.zeros(swath_data[i].shape(), dtype=bool)
            landmask[buff:-buff,buff:-buff] = (swath_data[i]['valid_land_doppler']==1)[buff:-buff,buff:-buff]
            dc_std = swath_data[i][swath_data[i]._get_band_number({'short_name':'dc_std'})]
            std_thres = np.nanmedian(dc_std[landmask]) + 2 * np.nanstd(dc_std[landmask])
            land_fdg.append(swath_data[i]['raw_fdg'][landmask * (dc_std <= std_thres)])
        dc_offset = np.nanmean(np.hstack(land_fdg))
        
        # add bias-corrected bands
        for i in range(n_subswaths):
            fdg = swath_data[i]['raw_fdg'] - dc_offset
            swath_data[i].add_band(array=fdg,
                parameters={'wkv':
                'surface_backwards_doppler_frequency_shift_of_radar_wave_due_to_surface_velocity'}
            )
            if swath_data[i].has_band('fww'):
                fww = swath_data[i]['fww']
            else:
                fww = 0
            theta = swath_data[i]['incidence_angle'] * np.pi / 180.
            vcurrent = -np.pi * (fdg-fww) / (wavenumber * np.sin(theta))
            vcurrent[swath_data[i]['valid_doppler']!=2] = np.nan
            swath_data[i].add_band(array=vcurrent,
                parameters={'wkv':'surface_radial_doppler_sea_water_velocity'})

        # Export data to netcdf
        for i in range(n_subswaths):
            print('Exporting %s (subswath %d)' %(swath_data[i].filename, i))
            fn = os.path.join(
                    ppath, 
                    os.path.basename(swath_data[i].filename).split('.')[0] 
                        + 'subswath%d.nc'%(i)
                )
            origFile = swath_data[i].filename
            try:
                swath_data[i].set_metadata(key='Originating file', value=origFile)
                swath_data[i].set_metadata(key='dc offset', value=dc_offset)
            except Exception as e:
                # TODO:
                warnings.warn('%s: BUG IN GDAL(?) - SHOULD BE CHECKED..'%e.message)
            swath_data[i].add_band(array=swath_data[i].get_geolocation_grids()[0],
                                   parameters={'name':'Lon'})
            swath_data[i].add_band(array=swath_data[i].get_geolocation_grids()[1],
                                   parameters={'name':'Lat'})
            swath_data[i].export(filename=fn)
            ncuri = os.path.join('file://localhost', fn)
            new_uri, created = DatasetURI.objects.get_or_create(uri=ncuri,
                    dataset=ds)


            '''
            # Maybe add figures in satellite projection...
            #filename = 'satproj_fdg_subswath_%d.png'%i
            #swath_data[i].write_figure(os.path.join(mp, filename),
            #        bands='fdg', clim=[-60,60], cmapName='jet')
            ## Add figure to db...
            
            # Reproject to leaflet projection
            xlon, xlat = swath_data[i].get_corners()
            d = Domain(NSR(3857),
                   '-lle %f %f %f %f -tr 1000 1000' % (
                        xlon.min(), xlat.min(), xlon.max(), xlat.max()))
            swath_data[i].reproject(d, eResampleAlg=1, tps=True)

            # Check if the reprojection failed
            try:
                inci = swath_data[i]['incidence_angle']
            except:
                is_corrupted = True
                warnings.warn('Could not read incidence angles - reprojection'\
                        ' failed')
                continue

            # Visualizations of the following bands (short_names) are created
            # when ingesting data:
            ingest_creates = [
                    'valid_doppler', 'valid_land_doppler', 'valid_sea_doppler',
                    'dca', 'fdg']
            if wind:
                ingest_creates.extend(['fww', 'Ur'])
            # (the geophysical doppler shift must later be added in a separate
            # manager method in order to estimate the range bias after
            # processing multiple files)
            for band in ingest_creates:
                filename = '%s_subswath_%d.png'%(band, i)
                # check uniqueness of parameter
                param = Parameter.objects.get(short_name = band)
                fig = swath_data[i].write_figure(os.path.join(mp, filename),
                    bands=band,
                    mask_array=swath_data[i]['swathmask'],
                    mask_lut={0:[128,128,128]}, transparency=[128,128,128])
                if type(fig)==Figure:
                    print 'Created figure of subswath %d, band %s' %(i, band)
                else:
                    warnings.warn('Figure NOT CREATED')

                # Get DatasetParameter
                dsp, created = DatasetParameter.objects.get_or_create(dataset=ds,
                    parameter = param)

                # Create Visualization
                try:
                    geom, created = GeographicLocation.objects.get_or_create(
                        geometry=WKTReader().read(swath_data[i].get_border_wkt()))
                except Exception as inst:
                    print(type(inst))
                    import ipdb
                    ipdb.set_trace()
                    raise
                vv, created = Visualization.objects.get_or_create(
                    uri='file://localhost%s/%s' % (mp, filename),
                    title='%s (swath %d)' %(param.standard_name, i+1),
                    geographic_location = geom
                )

                # Create VisualizationParameter
                vp, created = VisualizationParameter.objects.get_or_create(
                        visualization=vv, ds_parameter=dsp
                    )
            '''

        # create mosaiced png
        for i in range(n_subswaths):
            fn = os.path.join( ppath,
                     os.path.basename(swath_data[i].filename).split('.')[0] + 'subswath%d.nc'%(i) )
            swath_data[i] = Doppler(fn)
        #EPSG = 3857    # make sure wind correction is on!
        EPSG = 3995    # make sure wind correction is off!
        cellSize = 1000
        dcR = 30
        vR = 1
        s0 = []
        Lons, Lats = [], []
        for i in range(n_subswaths):
            s0.append(10*np.log10(swath_data[i]['corrected_sigma0_%s' % pol]).flatten())
            borderLon, borderLat = swath_data[i].get_border()
            Lons.append(borderLon)
            Lats.append(borderLat)
        s0min, s0max = np.percentile(np.concatenate(s0),[2.5,97.5])
        Lons = np.concatenate(Lons)
        Lats = np.concatenate(Lats)
        X = np.zeros(Lons.shape)
        Y = np.zeros(Lats.shape)
        for li, (lon,lat) in enumerate(zip(Lons,Lats)):
            X[li], Y[li] = LL2XY(EPSG, lon, lat)
        minX = np.min(X) - 5 * cellSize
        maxX = np.max(X) - 5 * cellSize
        minY = np.min(Y) + 5 * cellSize
        maxY = np.max(Y) + 5 * cellSize
        d = Domain(NSR(EPSG), '-te %f %f %f %f -tr %d %d' % (minX, minY, maxX, maxY, cellSize, cellSize))
        for i in range(n_subswaths):
            swath_data[i].reproject_GCPs()
            #swath_data[i].reproject(d, eResampleAlg=5, tps=True)
            swath_data[i].reproject(d, eResampleAlg=5, tps=False)
        merged_data = Nansat(domain=swath_data[0])
        merged_data.set_metadata(key='dc offset', value=dc_offset)
        corrected_sigma0 = np.ones((n_subswaths,merged_data.shape()[0],merged_data.shape()[1])) * np.nan
        fdg = np.ones((n_subswaths,merged_data.shape()[0],merged_data.shape()[1])) * np.nan
        Ur = np.ones((n_subswaths,merged_data.shape()[0],merged_data.shape()[1])) * np.nan
        for i in range(n_subswaths):
            corrected_sigma0[i] = swath_data[i]['corrected_sigma0_%s' % pol]
            fdg[i] = swath_data[i]['fdg']
            Ur[i] = swath_data[i]['Ur']

        fdgc = np.array([3.,0.,0.,0.,0.])
        fdgc -= np.nanmean(fdgc)
        for i in range(n_subswaths):
            corrected_sigma0[i] = swath_data[i]['corrected_sigma0_%s' % pol]
            fdg[i] = swath_data[i]['fdg'] + fdgc[i]
            Ur[i] = swath_data[i]['Ur']

        merged_data.add_band(array=np.nanmean(corrected_sigma0,axis=0),
            parameters={'name':'corrected_sigma0_%s' % pol,
                        'wkv':'surface_backwards_scattering_coefficient_of_radar_wave'})
        merged_data.add_band(array=np.nanmean(fdg,axis=0),
            parameters={'name':'fdg',
                        'wkv':'surface_backwards_doppler_frequency_shift_of_radar_wave_due_to_surface_velocity'})
        merged_data.add_band(array=np.nanmean(Ur,axis=0),
            parameters={'name':'Ur',
                        'wkv':'surface_radial_doppler_sea_water_velocity'})
        merged_data.add_band(array=merged_data.get_geolocation_grids()[0],
                             parameters={'name':'Lon'})
        merged_data.add_band(array=merged_data.get_geolocation_grids()[1],
                             parameters={'name':'Lat'})
                        
        fn = os.path.join(ppath,
                          os.path.basename(swath_data[i].filename).split('.')[0][:-9] + 'merged.nc')
        merged_data.export(filename=fn)

        from nansatmap import Nansatmap
        import matplotlib.pyplot as plt

        nmap = Nansatmap(d, resolution='i')
        nmap.draw_continents(zorder=0)
        nmap.drawmeridians(np.arange(-180,180,10),labels=[0,1,1,0],fontsize=5)
        nmap.drawparallels(np.arange(-90,90,5),labels=[1,0,0,1],fontsize=5)
        try:
            nmap.pcolormesh(10*np.log10(merged_data['corrected_sigma0_%s' % pol]),
                            vmin=s0min, vmax=s0max, cmap='gray')
        except:
            print('corrected_sigma0 cannot be drawn.')
        nmap.drawcoastlines(linewidth=0.5)
        nmap.add_colorbar(shrink=0.3)
        nmap.fig.savefig(
            os.path.join(mp, os.path.basename(swath_data[i].filename).split('.')[0][:-9] + '_sigma0.png'),
            bbox_inches='tight', pad_inches=0, dpi=300)
        plt.close('all')

        nmap = Nansatmap(d, resolution='i')
        nmap.draw_continents(zorder=0)
        nmap.drawmeridians(np.arange(-180,180,10),labels=[0,1,1,0],fontsize=5)
        nmap.drawparallels(np.arange(-90,90,5),labels=[1,0,0,1],fontsize=5)
        try:
            nmap.pcolormesh(merged_data['fdg'], vmin=-dcR, vmax=+dcR, cmap='jet')
        except:
            print('fdg cannot be drawn.')
        nmap.drawcoastlines(linewidth=0.5)
        nmap.add_colorbar(shrink=0.3)
        nmap.fig.savefig(
            os.path.join(mp, os.path.basename(swath_data[i].filename).split('.')[0][:-9] + '_fdg.png'),
            bbox_inches='tight', pad_inches=0, dpi=300)
        plt.close('all')
        
        nmap = Nansatmap(d, resolution='i')
        nmap.draw_continents(zorder=0)
        nmap.drawmeridians(np.arange(-180,180,10),labels=[0,1,1,0],fontsize=5)
        nmap.drawparallels(np.arange(-90,90,5),labels=[1,0,0,1],fontsize=5)
        try:
            nmap.pcolormesh(merged_data['Ur'], vmin=-vR, vmax=+vR, cmap='jet')
        except:
            print('Ur cannot be drawn.')
        nmap.drawcoastlines(linewidth=0.5)
        nmap.add_colorbar(shrink=0.3)
        nmap.fig.savefig(
            os.path.join(mp, os.path.basename(swath_data[i].filename).split('.')[0][:-9] + '_Ur.png'),
            bbox_inches='tight', pad_inches=0, dpi=300)
        plt.close('all')

        return ds, not is_corrupted


def LL2XY(EPSG, lon, lat):
    import ogr, osr
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(lon, lat)
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromEPSG(4326)
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(EPSG)
    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)
    point.Transform(coordTransform)
    return point.GetX(), point.GetY()

