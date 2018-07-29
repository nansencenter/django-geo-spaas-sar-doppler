from django.test import TestCase

from management.commands.ingest_sar_doppler import Command as IngestCommand
from managers import DatasetManager, gsar, Doppler
from models import Dataset as SARDAtaset
from django.core.management.base import CommandError
from django.core.management import call_command
from django.utils.six import StringIO
from geospaas.catalog.models import Dataset
from nansat import Domain
from datetime import datetime
from mock import patch
import numpy as np
import os


def generate_lat_lon(n, m):
    lat = np.array([range(n, m) for i in range(10)], dtype=np.int32)
    lon = np.array([[i] * (m - n) for i in range(10)], dtype=np.int32)
    return lat, lon


def mock_get_data(*args, **kwargs):
    if kwargs['channel'] == 0:
        lat, lon = generate_lat_lon(1, 4)
    if kwargs['channel'] == 1:
        lat, lon = generate_lat_lon(4, 7)
    if kwargs['channel'] == 2:
        lat, lon = generate_lat_lon(7, 11)

    return {'LATITUDE': lat, 'LONGITUDE': lon}


class TestDataset(TestCase):
    fixtures = ["vocabularies"]

    gsar_file_src = 'file://localhost/mnt/10.11.12.232/sat_downloads_asar/level-0/2010-01/ascending' \
                    '/VV/gsar_rvl/RVL_ASA_WS_20100101115616482.gsar'

    def setUp(self):
        self.ds, _ = SARDAtaset.objects.get_or_create(self.gsar_file_src, None, None,
                                                      start=datetime(1800, 1, 1),
                                                      end=datetime(2100, 1, 1),
                                                      reprocess=True)

    def test_file_added2db(self):
        self.assertEqual(len(Dataset.objects.all()), 1)

    def test_time_coverage(self):
        self.assertIsInstance(self.ds.time_coverage_start, str)
        self.assertEqual(self.ds.time_coverage_start, '2010-01-01T11:56:15.131639')
        self.assertIsInstance(self.ds.time_coverage_end, str)
        self.assertEqual(self.ds.time_coverage_end, '2010-01-01T11:57:18.303293')

    def test_polarization(self):
        self.assertEqual(self.ds.polarization, 'VV')

    def test_path(self):
        self.assertEqual(self.ds.sat_pass, 'ascending')

    def test_uri(self):
        self.assertEqual(len(self.ds.dataseturi_set.all()), 6)
        self.assertEqual(self.ds.dataseturi_set.first().uri, self.gsar_file_src)


class TestDatasetManager(TestCase):

    def test_check_intersection(self):
        # Domain inside image
        dom_1 = Domain(4326, '-lle 2 2 3 3 -tr 0.1 0.1')
        # Domain intersect border of image
        dom_2 = Domain(4326, '-lle 2 9 3 13 -tr 0.1 0.1')
        # Image inside the domain
        dom_3 = Domain(4326, '-lle -2 -2 13 13 -tr 0.1 0.1')
        # Image outside of the domain
        dom_4 = Domain(4326, '-lle 13 13 15 15 -tr 0.1 0.1')

        # Image domain
        img_dom = Domain(4326, '-lle 0 0 10 10 -tr 0.1 0.1')
        img_geom = img_dom.get_border_geometry()
        self.assertTrue(DatasetManager.check_intersection(dom_1, img_geom))
        self.assertTrue(DatasetManager.check_intersection(dom_2, img_geom))
        self.assertTrue(DatasetManager.check_intersection(dom_3, img_geom))
        self.assertFalse(DatasetManager.check_intersection(dom_4, img_geom))

    def test_assemble_domain_extent(self):
        extent_1 = {'lle': [1, 2, 3, 4], 'ts': [1, 2]}
        extent_2 = {'te': [1.1, 2, -3, 4], 'tr': [1, 0.2]}

        extent = DatasetManager.assemble_domain_extent(extent_1)
        self.assertIsInstance(extent, str)
        self.assertEqual(extent, '-lle 1 2 3 4 -ts 1 2')
        extent = DatasetManager.assemble_domain_extent(extent_2)
        self.assertEqual(extent, '-te 1.1 2 -3 4 -tr 1 0.2')

    def test_geometry_from_gsar(self):
        patcher = patch('sar_doppler.managers.gsar')
        patch_gsar = patcher.start()
        patch_gsar.return_value.getdata.side_effect = mock_get_data
        test_geom = DatasetManager.geometry_from_gsar('uri', 0)

        geom = 'POLYGON ((0 1,0 2,0 3,0 4,0 4,1 4,2 4,3 4,4 4,5 4,6 4,7 4,8 4,9 4,10 4,10 4,10 3,' \
               '10 2,10 1,10 1,9 1,8 1,7 1,6 1,5 1,4 1,3 1,2 1,1 1,0 1))'

        self.assertEqual(test_geom.ExportToWkt(), geom)

    def test_unite_geometry(self):
        patcher = patch('sar_doppler.managers.gsar')
        patch_gsar = patcher.start()
        patch_gsar.return_value.getdata.side_effect = mock_get_data

        test_geom = DatasetManager.unite_geometry('uri', 3)
        geom = 'POLYGON ((0 1,0 2,0 3,0 4,0 5,0 6,0 7,0 8,0 9,0 10,0 11,1 11,2 11,3 11,4 11,5 ' \
               '11,6 11,7 11,8 11,9 11,10 11,10 10,10 9,10 8,10 7,10 6,10 5,10 4,10 3,10 2,10 ' \
               '1,9 1,8 1,7 1,6 1,5 1,4 1,3 1,2 1,1 1,0 1))'

        self.assertEqual(test_geom.ExportToWkt(), geom)

    def test_get_pass_from_uri(self):
        uri1 = '/path/to/ascending/HH/gsar_rvl/file.gsar'
        uri2 = '/path/to/descending/HH/gsar_rvl/file.gsar'
        uri3 = '/path/to/no_pass_info/HH/gsar_rvl/file.gsar'
        uri4 = '/path/to/descendingHH/gsar_rvl/file.gsar'

        sat_pass = DatasetManager.get_pass_from_uri(uri1)
        self.assertIsInstance(sat_pass, str)
        self.assertEqual(sat_pass, 'ascending')

        sat_pass = DatasetManager.get_pass_from_uri(uri2)
        self.assertEqual(sat_pass, 'descending')

        sat_pass = DatasetManager.get_pass_from_uri(uri3)
        self.assertIsNone(sat_pass)

        sat_pass = DatasetManager.get_pass_from_uri(uri4)
        self.assertIsNone(sat_pass)

    def test_get_pol_from_uri(self):
        uri1 = '/path/to/ascending/HH/gsar_rvl/file.gsar'
        uri2 = '/path/to/descending/VV/gsar_rvl/file.gsar'
        uri3 = '/path/to/no_pass_info/no_pol_info/gsar_rvl/file.gsar'
        uri4 = '/path/to/descendingHH/gsar_rvl/file.gsar'

        sat_pass = DatasetManager.get_pol_from_uri(uri1)
        self.assertIsInstance(sat_pass, str)
        self.assertEqual(sat_pass, 'HH')

        sat_pass = DatasetManager.get_pol_from_uri(uri2)
        self.assertEqual(sat_pass, 'VV')

        sat_pass = DatasetManager.get_pol_from_uri(uri3)
        self.assertIsNone(sat_pass)

        sat_pass = DatasetManager.get_pol_from_uri(uri4)
        self.assertIsNone(sat_pass)

    def test_get_time_from_gsar(self):
        timestamp = u'2010-01-21T11:15:59.021842'
        patcher = patch('sar_doppler.managers.gsar')
        patch_gsar = patcher.start()
        patch_gsar.return_value.getinfo.return_value.gate = [{'YTIME': timestamp}]
        test = DatasetManager.get_time_from_gsar('uri')
        self.assertEqual(datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f'), test)

    def test_assemble_filename(self):
        ppath = '/product/path/'
        origin = '/path/to/the/file.gsar'
        swath_num = 1
        test_dest = DatasetManager.assemble_filename(ppath, origin, swath_num)
        self.assertIsInstance(test_dest, str)
        self.assertEqual(test_dest, '/product/path/filesubswath1.nc')

    def test_get_product_path(self):
        from django.conf import settings
        expected_ppath = os.path.join(settings.PRODUCTS_ROOT, 'managers', 'file')
        test_ppath = DatasetManager().get_product_path('/path/to/the/file.gsar')
        self.assertIsInstance(test_ppath, str)
        self.assertEqual(test_ppath, expected_ppath)



class TestIngestCommand(TestCase):

    fixtures = ["vocabularies"]

    gsar_file_src = 'file://localhost/mnt/10.11.12.232/sat_downloads_asar/' \
                    'level-0/2010-01/gsar_rvl/RVL_ASA_WS_20100110111600123.gsar'

    def test_command_options_1(self):

        out = StringIO()
        call_command('ingest_sar_doppler', self.gsar_file_src,
                     epsg=4326, lle=[3, 73.5, 3.2, 74], tr=[0.1, 0.1], with_domain=True,
                     reprocess=False, stdout=out)
        ds = Dataset.objects.all()
        self.assertEqual(len(ds), 1)

    def test_command_options_2(self):

        out = StringIO()
        call_command('ingest_sar_doppler', self.gsar_file_src,
                     epsg=4326, lle=[6, 70, 10, 75], tr=[0.1, 0.1], with_domain=True,
                     reprocess=False, stdout=out)
        ds = Dataset.objects.all()
        self.assertEqual(len(ds), 0)

    def test_check_srs(self):
        options_1 = {'epsg': None, 'proj4': None}
        options_2 = {'epsg': 4326, 'proj4': None}
        options_3 = {'epsg': None, 'proj4': 'test proj4 string'}
        options_4 = {'epsg': 4326, 'proj4': 'test proj4 string'}

        srs = IngestCommand.check_srs(options_1)
        self.assertEqual(srs, None)

        srs = IngestCommand.check_srs(options_2)
        self.assertEqual(srs, 4326)

        srs = IngestCommand.check_srs(options_3)
        self.assertEqual(srs, 'test proj4 string')

        with self.assertRaises(CommandError) as opt_err:
            srs = IngestCommand.check_srs(options_4)
            self.assertEqual(opt_err.message, 'Only one Spatial reference system'
                                              ' can be used (EPSG or PROJ4)')

    def test_check_extent_pairs(self):
        extent = {}
        param1, param2 = 'lle', 'te'
        options_1 = {'lle': None, 'te': None}
        options_2 = {'lle': [1, 2, 3, 4], 'te': None}
        options_3 = {'lle': None, 'te': [1, 2, 3, 4]}
        options_4 = {'lle': [1, 2, 3, 4], 'te': [1, 2, 3, 4]}
        options_5 = {'tr': [1, 2], 'ts': [1, 2]}

        extent = IngestCommand.check_extent_pairs(extent, options_1, param1, param2)
        self.assertEqual(extent, None)

        extent = {}
        extent = IngestCommand.check_extent_pairs(extent, options_2, param1, param2)
        self.assertEqual(extent['lle'], [1, 2, 3, 4])

        extent = {}
        extent = IngestCommand.check_extent_pairs(extent, options_3, param1, param2)
        self.assertEqual(extent['te'], [1, 2, 3, 4])

        extent = {}
        with self.assertRaises(CommandError) as opt_err:
            extent = IngestCommand.check_extent_pairs(extent, options_4, param1, param2)
            self.assertEqual(opt_err.message, '--lle cannot be used with --te')

        extent = {}
        param1, param2 = 'tr', 'ts'
        with self.assertRaises(CommandError) as opt_err:
            extent = IngestCommand.check_extent_pairs(extent, options_5, param1, param2)
            self.assertEqual(opt_err.message, '--tr cannot be used with --ts')

    def test_validate_domain(self):

        options_1 = {'epsg': None, 'proj4': None, 'lle': None, 'te': [1, 2, 3, 4]}
        with self.assertRaises(CommandError) as opt_err:
            srs, extent = IngestCommand.validate_domain(options_1)
            self.assertEqual(opt_err.message, 'Spatial reference was not specified')

        options_2 = {'epsg': 4326, 'proj4': None, 'lle': None, 'te': None}
        with self.assertRaises(CommandError) as opt_err:
            srs, extent = IngestCommand.validate_domain(options_2)
            self.assertEqual(opt_err.message, '--lle or --te was not specified')

        options_3 = {'epsg': None, 'proj4': None, 'lle': None,
                     'te': [1, 2, 3, 4], 'tr': None, 'ts': None}
        with self.assertRaises(CommandError) as opt_err:
            srs, extent = IngestCommand.validate_domain(options_3)
            self.assertEqual(opt_err.message, '--tr or --ts was not specified')

        options_4 = {'epsg': 4326, 'proj4': None, 'lle': None,
                     'te': [1, 2, 3, 4], 'tr': None, 'ts': [1, 2]}

        srs, extent = IngestCommand.validate_domain(options_4)
        self.assertEqual(srs, 4326)
        self.assertIsInstance(extent, dict)
        self.assertEqual(extent['te'], [1, 2, 3, 4])
        self.assertEqual(extent['ts'], [1, 2])

    def test_parse_time(self):
        timestamp1 = '2010-01-01'
        test_out1 = IngestCommand.parse_date(timestamp1)
        self.assertIsInstance(test_out1, datetime)
        self.assertEqual(test_out1, datetime(2010, 1, 1))
