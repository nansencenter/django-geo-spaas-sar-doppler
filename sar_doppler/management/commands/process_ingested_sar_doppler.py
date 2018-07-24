''' Processing of SAR Doppler from Norut's GSAR '''
from django.core.management.base import BaseCommand

from nansat.exceptions import NansatGeolocationError

from sar_doppler.models import Dataset

class Command(BaseCommand):
    help = 'Post-processing of ingested GSAR RVL files and generation of png images for ' \
            'display in Leaflet'

    # Reprocessing should probably be a separate command
    #def add_arguments(self, parser):
    #    parser.add_argument('--reprocess', action='store_true', 
    #            help='Force reprocessing')

    def handle(self, *args, **options):
        unprocessed = Dataset.objects.filter(
                entry_title='SAR Doppler').exclude(dataseturi__uri__endswith='.nc')
        num_unprocessed = len(unprocessed)

        for i,ds in enumerate(unprocessed):
            uri = ds.dataseturi_set.get(uri__endswith='.gsar').uri
            try:
                updated_ds, corrupted = Dataset.objects.process(uri)
            except (ValueError, IOError, NansatGeolocationError):
                # some files manually moved to *.error...
                continue
            if not corrupted:
                self.stdout.write('Successfully processed (%d/%d): %s\n' % (i+1, num_unprocessed,
                    uri))
            else:
                self.stdout.write('Corrupt file (%d/%d, may have been partly processed): %s\n' %(i,
                    num_unprocessed, uri))

