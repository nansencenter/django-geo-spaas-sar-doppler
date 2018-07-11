import os, warnings
from math import sin, pi, cos, acos, copysign
import numpy as np
from scipy.ndimage.filters import median_filter

from dateutil.parser import parse
from datetime import timedelta

from django.conf import settings
from django.utils import timezone
from django.db import models
from django.contrib.gis.geos import WKTReader, Polygon, MultiPolygon

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

    NUM_SUBSWATS = 5
    NUM_BORDER_POINTS = 10

    def list_of_coordinates(self, left, right, upper, lower, axis):
        coord_list = np.concatenate(
            (left[axis][0], upper[axis][0], upper[axis][1], upper[axis][2],
             upper[axis][3], upper[axis][4], np.flipud(right[axis][4]),
             np.flipud(lower[axis][4]), np.flipud(lower[axis][3]),
             np.flipud(lower[axis][2]), np.flipud(lower[axis][1]),
             np.flipud(lower[axis][0])))

        return coord_list

    def update_borders(self, swath_data, i, step, border):
        """
        Add information about borders coordinates and steps size from the swath
        :param swath_data: sardoppler.sardoppler.Doppler object, Data from one swath
        :param i: int, number of swath
        :param step: dict, Dictionary for accumulation of maximal steps in azimuthal and range direction
        :param border: dict, Dictionary for accumulation of lat/con coordinates for each border;
        left and right are azimuthal borders; upper and bottom are range borders
        :return: dict, dict, <step> and <border> dictionaries updated with values from the input swath
        """
        lon, lat = swath_data.get_geolocation_grids()

        step['azimuth'][i] = max(1, (lon.shape[0] / 2 * 2 - 1) / self.NUM_BORDER_POINTS)
        step['range'][i] = max(1, (lon.shape[1] / 2 * 2 - 1) / self.NUM_BORDER_POINTS)

        border['left']['lon'][i] = lon[0:-1:step['azimuth'][i], 0]
        border['left']['lat'][i] = lat[0:-1:step['azimuth'][i], 0]

        border['right']['lon'][i] = lon[0:-1:step['azimuth'][i], -1]
        border['right']['lat'][i] = lat[0:-1:step['azimuth'][i], -1]

        border['upper']['lon'][i] = lon[-1, 0:-1:step['range'][i]]
        border['upper']['lat'][i] = lat[-1, 0:-1:step['range'][i]]

        border['lower']['lon'][i] = lon[0, 0:-1:step['range'][i]]
        border['lower']['lat'][i] = lat[0, 0:-1:step['range'][i]]

        return step, border

    def get_or_create(self, uri, srs, extent_dict, reprocess=False, *args, **kwargs):

        # ingest file to db
        ds, created = super(DatasetManager, self).get_or_create(uri, *args, **kwargs)
        if not type(ds) == Dataset:
            return ds, False

        # Assemble domain
        if srs and extent_dict:
            extent_str = DatasetManager.assemble_domain_extent(extent_dict)
            dom = Domain(srs, extent_str)
            spec_domain = True

        # set Dataset entry_title
        ds.entry_title = 'SAR Doppler'
        ds.save()

        fn = nansat_filename(uri)
        n = Nansat(fn, subswath=0)
        gg = WKTReader().read(n.get_border_wkt())
        if ds.geographic_location.geometry.area > gg.area and not reprocess:
            return ds, False

        ''' Update dataset border geometry

        This must be done every time a Doppler file is processed. It is time
        consuming but apparently the only way to do it. Could be checked
        though...
        '''
        swath_data = {}

        steps = {
            'azimuth': {},
            'range': {}
        }
        # Dictionaries for accumulation of image border coordinates

        borders = {
            'left': {'lat': {}, 'lon': {}},  # Azimuthal direction
            'right': {'lat': {}, 'lon': {}},  # Azimuthal direction
            'upper': {'lat': {}, 'lon': {}},  # Range direction
            'lower': {'lat': {}, 'lon': {}}  # Range direction
        }

        # Flag for detection of corruption in img
        not_corrupted = True

        for i in range(self.NUM_SUBSWATS):
            # Read subswath
            swath_data[i] = Nansat(fn, subswath=i)

            # Should use nansat.domain.get_border - see nansat issue #166
            # (https://github.com/nansencenter/nansat/issues/166)
            steps, borders = self.update_borders(swath_data[i], i, steps, borders)

        lons = self.list_of_coordinates(borders['left'], borders['right'],
                                        borders['upper'], borders['lower'], 'lon')
        # apply 180 degree correction to longitude - code copied from
        # get_border_wkt...

        # TODO: This loop returns exactly the same list of lons
        for ilon, llo in enumerate(lons):
            lons[ilon] = copysign(acos(cos(llo * pi / 180.)) / pi * 180, sin(llo * pi / 180.))

        lats = self.list_of_coordinates(borders['left'], borders['right'],
                                        borders['upper'], borders['lower'], 'lat')

        # Create a polygon form lats and lons
        new_geometry = Polygon(zip(lons, lats))

        # Get geolocation of dataset - this must be updated
        geoloc = ds.geographic_location

        # Check geometry, return if it is the same as the stored one
        if geoloc.geometry == new_geometry and not reprocess:
            return ds, True

        if geoloc.geometry != new_geometry:
            # Change the dataset geolocation to cover all subswaths
            geoloc.geometry = new_geometry
            geoloc.save()

        # Create data products
        # mm = self.__module__.split('.')
        # module = '%s.%s' % (mm[0], mm[1])
        # local uri path for visualizations
        # mp = media_path(module, swath_data[i].fileName)
        # ppath = product_path(module, swath_data[i].fileName)

        # for i in range(self.NUM_SUBSWATS):
        #    self.generate_product(swath_data[i], i, ppath, mp, ds)

        # return ds, not_corrupted
        return ds, not_corrupted

    @staticmethod
    def assemble_domain_extent(extent_dict):

        extent_param = 'lle' if 'lle' in extent_dict.keys() else 'te'
        resolution_param = 'ts' if 'ts' in extent_dict.keys() else 'tr'

        extent_str = '-%s %s -%s %s' % (extent_param,
                                        ' '.join([str(i) for i in extent_dict[extent_param]]),
                                        resolution_param,
                                        ' '.join([str(i) for i in extent_dict[resolution_param]]))

        return extent_str

    @staticmethod
    def unite_geometry(uri, subswaths_num):
        n = Nansat(uri, subswath=0)
        poly_base = n.get_border_geometry()
        for subswath_num in range(1, subswaths_num):
            n = Nansat(uri, subswath=subswath_num)
            poly_base = poly_base.Union(n.get_border_geometry())
        return poly_base
