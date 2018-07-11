from django.test import TestCase
from management.commands.ingest_sar_doppler import Command as IngestCommand
from django.core.management.base import CommandError


class TestIngestCommand(TestCase):

    def test_check_srs(self):
        options_1 = {'epsg': None, 'proj4': None}
        options_2 = {'epsg': 4326, 'proj4': None}
        options_3 = {'epsg': None, 'proj4': 'test proj4 string'}
        options_4 = {'epsg': 4326, 'proj4': 'test proj4 string'}

        valid_srs, srs = IngestCommand.check_srs(options_1)
        self.assertFalse(valid_srs)
        self.assertEqual(srs, None)

        valid_srs, srs = IngestCommand.check_srs(options_2)
        self.assertTrue(valid_srs)
        self.assertEqual(srs, 4326)

        valid_srs, srs = IngestCommand.check_srs(options_3)
        self.assertTrue(valid_srs)
        self.assertEqual(srs, 'test proj4 string')

        with self.assertRaises(CommandError) as opt_err:
            valid_srs, srs = IngestCommand.check_srs(options_4)
            self.assertEqual(opt_err.message, 'Only one Spatial reference system'
                                              ' can be used (EPSG or PROJ4)')



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


