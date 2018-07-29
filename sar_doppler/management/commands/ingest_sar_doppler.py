''' Processing of SAR Doppler from Norut's GSAR '''
from base_sar_doppler import Command as BaseCommand

from geospaas.utils import uris_from_args
from geospaas.catalog.models import Dataset as catalogDataset

from sar_doppler.models import Dataset


class Command(BaseCommand):
    args = '<filename>'
    # TODO: The description should be updated with new functionality and examples. Artem. 10-07-18
    help = 'Add WS file to catalog archive and make png images for display in Leaflet'

    def add_arguments(self, parser):
        # Inherit standard arguments
        super(Command, self).add_arguments(parser)
        # Input files
        parser.add_argument('gsar_files', nargs='*', type=str)

    def handle(self, *args, **options):

        options['start'] = Command.parse_date(options['start'])
        options['end'] = Command.parse_date(options['end'])

        srs, extent = None, None
        if options['with_domain'] is True:
            srs, extent = Command.validate_domain(options)
            self.stdout.write('Domain parameters are valid')
        uris_num = len(uris_from_args(options['gsar_files']))
        uri_id = 1
        for non_ingested_uri in uris_from_args(options['gsar_files']):
            print('Processed: %s / %s' % (uri_id, uris_num))
            self.stdout.write('Ingesting %s ...\n' % non_ingested_uri)
            try:
                ds, cr = Dataset.objects.get_or_create(non_ingested_uri, srs, extent, **options)
            except Exception as ex:
                self.stdout.write(ex.message)
                uri_id += 1
                self.stdout.write('ERROR : %s\n' % non_ingested_uri)
                continue
            if ds is None:

                self.stdout.write('Does not intersect with a required domain: %s\n'
                                  % non_ingested_uri)
            elif not type(ds) == catalogDataset:
                self.stdout.write('Not found: %s\n' % non_ingested_uri)
            elif cr:
                self.stdout.write('Successfully added: %s\n' % non_ingested_uri)
            else:
                self.stdout.write('Was already added: %s\n' % non_ingested_uri)
                if not type(ds) == catalogDataset:
                    self.stdout.write('Not found: %s\n' % non_ingested_uri)
                elif cr:
                    self.stdout.write('Successfully added: %s\n' % non_ingested_uri)
                else:
                    self.stdout.write('Was already added: %s\n' % non_ingested_uri)
            uri_id += 1
