import os, warnings
from django.contrib.gis.geos import WKTReader
from django.db import models

from geospaas.utils import nansat_filename, media_path, product_path
from geospaas.catalog.models import Dataset, DatasetURI
from geospaas.nansat_ingestor.managers import DatasetManager as NansatIngestorManager

from nansat import Nansat, Domain
from sardoppler.gsar import gsar
from sardoppler.sardoppler import Doppler
from datetime import datetime


class DatasetManager(models.Manager):

    NUM_SUBSWATS = 5
    NUM_BORDER_POINTS = 10

    def get_or_create(self, uri, srs, extent_dict, *args, **kwargs):
        filename = nansat_filename(uri)

        # Check time
        time_coverage = DatasetManager.get_time_from_gsar(filename)
        if time_coverage > kwargs['end'] or time_coverage < kwargs['start']:
            return None, True

        # Assemble domain
        spec_domain = False
        if srs and extent_dict:
            extent_str = DatasetManager.assemble_domain_extent(extent_dict)
            dom = Domain(srs, extent_str)
            spec_domain = True

        image_geometry = DatasetManager.unite_geometry(filename, self.NUM_SUBSWATS)

        if spec_domain:
            intersection = DatasetManager.check_intersection(dom, image_geometry)
            if not intersection:
                return None, True

        ds, cr = NansatIngestorManager().get_or_create(uri, *args, **kwargs)
        if not type(ds) == Dataset:
            return ds, False

        # ingest file to sar_doppler db
        ds, created = super(DatasetManager, self).get_or_create(
            entry_title='SAR Doppler',
            ISO_topic_category=ds.ISO_topic_category,
            data_center=ds.data_center,
            summary=ds.summary,
            time_coverage_start=ds.time_coverage_start,
            time_coverage_end=ds.time_coverage_end,
            source=ds.source,
            geographic_location=ds.geographic_location,
            sat_pass=DatasetManager.get_pass_from_uri(uri),
            polarization=DatasetManager.get_pol_from_uri(uri))

        ds.save()
        _ = DatasetURI.objects.get_or_create(uri=uri, dataset=ds)[0]
        not_corrupted = True

        n = Nansat(filename, subswath=0)
        gg = WKTReader().read(n.get_border_wkt())

        # Get geolocation of dataset - this must be updated
        geoloc = ds.geographic_location

        # Check geometry, return if it is the same as the stored one

        if geoloc.geometry != image_geometry.ExportToIsoWkt():
            # Change the dataset geolocation to cover all subswaths
            geoloc.geometry = image_geometry.ExportToIsoWkt()
            geoloc.save()

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
    def geometry_from_gsar(uri, subswath):
        gsar_file = gsar(uri)
        lat = gsar_file.getdata(channel=subswath)['LATITUDE']
        lon = gsar_file.getdata(channel=subswath)['LONGITUDE']
        dom = Domain(lon=lon, lat=lat)
        return dom.get_border_geometry()

    @staticmethod
    def unite_geometry(uri, subswaths_num):
        poly_base = DatasetManager.geometry_from_gsar(uri, 0)
        for subswaths_num in range(1, subswaths_num):
            swant_geomety = DatasetManager.geometry_from_gsar(uri, subswaths_num)
            poly_base = poly_base.Union(swant_geomety)
        return poly_base

    @staticmethod
    def check_intersection(dom, img_geometry):
        return img_geometry.Intersect(dom.get_border_geometry())

    @staticmethod
    def get_pass_from_uri(uri):
        chopped_uri = uri.split('/')
        if 'ascending' in chopped_uri:
            sat_pass = 'ascending'
        elif 'descending' in chopped_uri:
            sat_pass = 'descending'
        else:
            sat_pass = None
            warnings.warn('Can not extract satellite pass from uri')
        return sat_pass

    @staticmethod
    def get_pol_from_uri(uri):
        chopped_uri = uri.split('/')
        if 'HH' in chopped_uri:
            polarization = 'HH'
        elif 'VV' in chopped_uri:
            polarization = 'VV'
        else:
            polarization = None
            warnings.warn('Can not extract polarization from uri')
        return polarization

    @staticmethod
    def get_time_from_gsar(uri):
        gsar_file = gsar(uri)
        metadata = gsar_file.getinfo(channel=0).gate[0]['YTIME']
        return datetime.strptime(metadata, '%Y-%m-%dT%H:%M:%S.%f')
