from mock import patch, Mock, DEFAULT

from django.test import TestCase
from django.core.management import call_command
from django.utils.six import StringIO

from sar_doppler.models import Dataset
from sar_doppler.managers import DatasetManager

class TestProcessingSARDoppler(TestCase):

    fixtures = ["vocabularies"]

    #def test_process_sar_doppler(self):
    #    out = StringIO()
    #    wf = 'file://localhost/mnt/10.11.12.231/sat_auxdata/model/ncep/gfs/' \
    #            'gfs20091116/gfs.t18z.master.grbf03'
    #    call_command('ingest', wf, stdout=out)
    #    f = 'file://localhost/mnt/10.11.12.231/sat_downloads_asar/level-0/' \
    #            'gsar_rvl/RVL_ASA_WS_20091116195940116.gsar'
    #    call_command('ingest_sar_doppler', f, stdout=out)
    #    self.assertIn('Successfully added:', out.getvalue())

    @patch.multiple(DatasetManager, filter=DEFAULT, process=DEFAULT, exclude=Mock(return_value=None))
    def test_process_ingested_sar_doppler(self, filter, process):
        #mock_ds_objects.filter.return_value = mock_ds_objects
        #mock_ds_objects.exclude.return_value = mock_ds_objects
        #mock_ds_objects.process.return_value = (mock_ds_objects, True)

        out = StringIO()
        call_command('process_ingested_sar_doppler', stdout=out)
        filter.assert_called()
        #exclude.assert_called_once()
        process.assert_called_once()
