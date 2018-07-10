from django.test import TestCase
from django.core.management import call_command
from django.utils.six import StringIO
import sys


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


class TestIngestSARDopplerCommand(TestCase):
    fixtures = ["vocabularies"]

    def test_command_options(self):
        out = StringIO()
        gsar_file_src = '/mnt/10.11.12.232/sat_downloads_asar/level-0/2010-01/gsar_rvl/RVL_ASA_WS_20100110111600123.gsar'
        call_command('ingest_sar_doppler', gsar_file_src, epsg=4326, reprocess=False, stdout=out)
        # self.assertIsInstance(out['epsg'], int)
        # self.assertEqual(())
        output = out.getvalue()
        self.assertIsInstance(output['epsg'], int)
