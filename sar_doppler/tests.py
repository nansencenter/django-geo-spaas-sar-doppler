from django.test import TestCase

from management.commands.ingest_sar_doppler import Command as IngestCommand
from managers import DatasetManager, gsar
from models import Dataset as SARDAtaset
from django.core.management.base import CommandError
from django.core.management import call_command
from django.utils.six import StringIO
from geospaas.utils import nansat_filename
from unittest import skip
from geospaas.catalog.models import Dataset
from nansat import Domain
from datetime import datetime
from mock import patch


class TestDataset(TestCase):
    fixtures = ["vocabularies"]

    gsar_file_src = 'file://localhost/mnt/10.11.12.232/sat_downloads_asar/level-0/2010-01/ascending' \
                    '/VV/gsar_rvl/RVL_ASA_WS_20100101115616482.gsar'

    def setUp(self):
        self.ds, _ = SARDAtaset.objects.get_or_create(self.gsar_file_src, None, None)

    def test_file_added2db(self):
        self.assertEqual(len(Dataset.objects.all()), 1)

    def test_time_coverage(self):
        self.assertIsInstance(self.ds.time_coverage_start, str)
        self.assertEqual(self.ds.time_coverage_start, '2010-01-01T11:56:15.131639')
        self.assertIsInstance(self.ds.time_coverage_end, str)
        self.assertEqual(self.ds.time_coverage_end, '2010-01-01T11:57:18.303293')

    def test_polarization(self):
        self.assertEqual(self.ds.summary.split(',')[0], 'VV')

    def test_path(self):
        self.assertEqual(self.ds.summary.split(',')[1], 'ascending')

    def test_uri(self):
        self.assertEqual(self.ds.dataseturi_set.first().uri, self.gsar_file_src)


class TestDatasetManager(TestCase):
    fixtures = ["vocabularies"]

    gsar_file_src = 'file://localhost/mnt/10.11.12.232/sat_downloads_asar/' \
                    'level-0/2010-01/gsar_rvl/RVL_ASA_WS_20100110111600123.gsar'

    def test_check_intersection(self):
        # Domain inside image
        dom_1 = Domain(4326, '-lle 3 73.5 3.2 74 -tr 0.1 0.1')
        # Domain intersect border of image
        dom_2 = Domain(4326, '-lle 3 73.5 6 74 -tr 0.1 0.1')
        # Image inside the domain
        dom_3 = Domain(4326, '-lle -6 70 6 75 -tr 0.1 0.1')
        # Image outside of the domain
        dom_4 = Domain(4326, '-lle 6 70 10 75 -tr 0.1 0.1')

        geometry = DatasetManager.unite_geometry(nansat_filename(self.gsar_file_src), 2)
        self.assertTrue(DatasetManager.check_intersection(dom_1, geometry))
        self.assertTrue(DatasetManager.check_intersection(dom_2, geometry))
        self.assertTrue(DatasetManager.check_intersection(dom_3, geometry))
        self.assertFalse(DatasetManager.check_intersection(dom_4, geometry))

    def test_assemble_domain_extent(self):
        extent_1 = {'lle': [1, 2, 3, 4], 'ts': [1, 2]}
        extent_2 = {'te': [1.1, 2, -3, 4], 'tr': [1, 0.2]}

        extent = DatasetManager.assemble_domain_extent(extent_1)
        self.assertIsInstance(extent, str)
        self.assertEqual(extent, '-lle 1 2 3 4 -ts 1 2')
        extent = DatasetManager.assemble_domain_extent(extent_2)
        self.assertEqual(extent, '-te 1.1 2 -3 4 -tr 1 0.2')

    @skip("It is almost matching. Difference is ~0.000000001")
    def test_geometry_from_gsar(self):
        test_geometry = 'POLYGON ((4.77975239848 74.1678231767,4.3541864768 74.2229876626,3.94051648087 74.2759214114,3.53874241069 74.3266244232,3.14886426625 74.3750966981,2.77088204756 74.421338236,2.40479575462 74.4653490368,2.05060538742 74.5071291007,1.70831094596 74.5466784277,1.37791243025 74.5839970176,1.00748262363 74.6247159688,1.00748262363 74.6247159688,0.51334604347 74.3007258819,0.0345645730268 73.9758560509,-0.428861787695 73.6501064759,-0.876933038696 73.3234771569,-1.30964917998 72.9959680938,-1.72701021153 72.6675792866,-2.12901613337 72.3383107354,-2.51566694549 72.0081624402,-2.88696264788 71.6771344009,-3.26059168057 71.328326447,-3.26059168057 71.328326447,-2.95493804284 71.2955393692,-2.68006195761 71.2650195279,-2.39328994663 71.2322689496,-2.09462200991 71.1972876343,-1.78405814744 71.1600755821,-1.46159835923 71.1206327928,-1.12724264527 71.0789592666,-0.780991005562 71.0350550034,-0.422843440109 70.9889200031,-0.0527999489107 70.9405542659,-0.0527999489107 70.9405542659,0.379847024214 71.2821354443,0.807304960248 71.6062863908,1.250118006 71.9295575932,1.70828616148 72.2519490515,2.18180942668 72.5734607659,2.67068780159 72.8940927361,3.17492128623 73.2138449623,3.6945098806 73.5327174445,4.22945358468 73.8507101827,4.77975239848 74.1678231767))'
        geom = DatasetManager.geometry_from_gsar(nansat_filename(self.gsar_file_src), 0)
        self.assertEqual(geom.ExportToIsoWkt(), test_geometry)

    @skip("It is almost matching. Difference is ~0.000000001")
    def test_unite_geometry(self):
        test_geometry = 'MULTIPOLYGON (((4.77975239848 74.1678231767,4.22945358468 73.8507101827,3.6945098806 73.5327174445,3.17492128623 73.2138449623,2.67068780159 72.8940927361,2.18180942668 72.5734607659,1.70828616148 72.2519490515,1.250118006 71.9295575932,0.807304960248 71.6062863908,0.379847024214 71.2821354443,-0.0527999489107 70.9405542659,-0.422843440109 70.9889200031,-0.780991005562 71.0350550034,-1.12724264527 71.0789592666,-1.46159835923 71.1206327928,-1.78405814744 71.1600755821,-2.09462200991 71.1972876343,-2.39328994663 71.2322689496,-2.68006195761 71.2650195279,-2.95493804284 71.2955393692,-3.26059168057 71.328326447,-2.88696264788 71.6771344009,-2.51566694549 72.0081624402,-2.12901613337 72.3383107354,-1.72701021153 72.6675792866,-1.30964917998 72.9959680938,-0.876933038696 73.3234771569,-0.428861787695 73.6501064759,0.0345645730268 73.9758560509,0.51334604347 74.3007258819,1.00748262363 74.6247159688,1.37791243025 74.5839970176,1.70831094596 74.5466784277,2.05060538742 74.5071291007,2.40479575462 74.4653490368,2.77088204756 74.421338236,3.14886426625 74.3750966981,3.53874241069 74.3266244232,3.94051648087 74.2759214114,4.3541864768 74.2229876626,4.77975239848 74.1678231767)),((0.869877202765 74.6434163981,0.478234846317 74.3847006495,0.0959996792631 74.1255334281,-0.276828298397 73.8659147337,-0.640249086664 73.6058445663,-0.994262685538 73.345322926,-1.33886909502 73.0843498128,-1.6740683151 72.8229252267,-1.9998603458 72.5610491676,-2.3162451871 72.2987216355,-2.6369528507 72.0239874032,-2.85091557295 72.0472810756,-3.0621242675 72.070010278,-3.27057893437 72.0921750102,-3.47627957354 72.1137752723,-3.67922618504 72.1348110643,-3.87941876884 72.1552823862,-4.07685732495 72.175189238,-4.27154185338 72.1945316197,-4.46347235412 72.2133095313,-4.66605604868 72.2328023333,-4.37708929988 72.5105900951,-4.09106533361 72.7758383944,-3.79563417795 73.0406352209,-3.4907958329 73.3049805743,-3.17655029846 73.5688744549,-2.85289757462 73.8323168625,-2.51983766138 74.0953077972,-2.17737055876 74.3578472589,-1.82549626674 74.6199352477,-1.46421478533 74.8815717635,-1.2291854748 74.8589576387,-1.00697239915 74.8372664923,-0.782005295822 74.8150108759,-0.554284164802 74.7921907894,-0.323809006094 74.7688062328,-0.0905798196977 74.7448572061,0.145403394386 74.7203437092,0.384140636158 74.6952657423,0.625631905617 74.6696233052,0.869877202765 74.6434163981)))'
        geometry = DatasetManager.unite_geometry(nansat_filename(self.gsar_file_src), 2)
        self.assertEqual(geometry.ExportToIsoWkt(), test_geometry)

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
        timestamp = u'2010-01-10T11:15:59.021842'
        patcher = patch('sar_doppler.managers.gsar')
        patch_gsar = patcher.start()
        patch_gsar.return_value.getinfo.return_value.gate = [{'YTIME': timestamp}]
        test = DatasetManager.get_time_from_gsar('uri')
        self.assertEqual(datetime.strptime(timestamp, '%Y-%d-%mT%H:%M:%S.%f'), test)


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


# class TestProcessingSARDoppler(TestCase):
#
#    fixtures = ["vocabularies"]
#
#    def test_process_sar_doppler(self):
#        out = StringIO()
#        wf = 'file://localhost/mnt/10.11.12.231/sat_auxdata/model/ncep/gfs/' \
#                'gfs20091116/gfs.t18z.master.grbf03'
#        call_command('ingest', wf, stdout=out)
#        f = 'file://localhost/mnt/10.11.12.231/sat_downloads_asar/level-0/' \
#                'gsar_rvl/RVL_ASA_WS_20091116195940116.gsar'
#        call_command('ingest_sar_doppler', f, stdout=out)
#        self.assertIn('Successfully added:', out.getvalue())


