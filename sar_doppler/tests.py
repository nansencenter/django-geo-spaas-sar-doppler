from django.test import TestCase
from management.commands.ingest_sar_doppler import Command as IngestCommand
from django.core.management.base import CommandError


class TestIngestCommand(TestCase):

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

from django.core.management import call_command
from django.utils.six import StringIO
from managers import DatasetManager

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


#class TestIngestSARDopplerCommand(TestCase):
#    fixtures = ["vocabularies"]
#
#    gsar_file_src = 'file://localhost/mnt/10.11.12.232/sat_downloads_asar/' \
#                    'level-0/2010-01/gsar_rvl/RVL_ASA_WS_20100110111600123.gsar'
#
#    def test_command_options(self):
#        out = StringIO()
#        call_command('ingest_sar_doppler', self.gsar_file_src,
#                     epsg=4326, reprocess=False, stdout=out)
#
#    def test_domain(self):
#        pass


