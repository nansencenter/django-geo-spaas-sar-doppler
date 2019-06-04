''' Processing of SAR Doppler from Norut's GSAR '''
import logging
from django.core.management.base import BaseCommand

from nansat.exceptions import NansatGeolocationError

from sar_doppler.models import Dataset

logging.basicConfig(filename='process_ingested_sar_doppler.log', level=logging.INFO)

class Command(BaseCommand):
    help = 'Post-processing of ingested GSAR RVL files and generation of png images for ' \
            'display in Leaflet'

    def add_arguments(self, parser):
        parser.add_argument('--file', type=str, default='')
    #    Reprocessing should probably be a separate command
    #    parser.add_argument('--reprocess', action='store_true', 
    #            help='Force reprocessing')

    def handle(self, *args, **options):
        unprocessed = Dataset.objects.filter(
                entry_title='SAR Doppler',
                dataseturi__uri__contains=options['file']
            ).exclude(
                dataseturi__uri__endswith='.nc'
            )
        num_unprocessed = len(unprocessed)

        print('Processing %d datasets' %num_unprocessed)
        for i,ds in enumerate(unprocessed):
            uri = ds.dataseturi_set.get(uri__endswith='.gsar').uri
            try:
                updated_ds, processed = Dataset.objects.process(uri)
            except (ValueError, IOError, NansatGeolocationError):
                # some files manually moved to *.error...
                continue
            if processed:
                self.stdout.write('Successfully processed (%d/%d): %s\n' % (i+1, num_unprocessed,
                    uri))
            else:
                msg = 'Corrupt file (%d/%d, may have been partly processed): %s\n' %(i+1,
                    num_unprocessed, uri)
                logging.info(msg)
                self.stdout.write(msg)

