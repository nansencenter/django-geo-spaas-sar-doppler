# Processing of SAR Doppler from Norut's GSAR

from geospass.utils import ProcessingBaseCommand
from sar_doppler.toolbox.reprocessing import DatasetProcessor
from sar_doppler.models import Dataset
from sar_doppler.managers import DatasetManager
from nansat import Domain


class Command(ProcessingBaseCommand):
    args = '<filename>'
    # TODO: The description should be updated with new functionality and examples. Artem. 10-07-18
    help = 'Add WS file to catalog archive and make png images for display in Leaflet'

    def add_arguments(self, parser):
        # Inherit standard arguments
        super(Command, self).add_arguments(parser)

        parser.add_argument('--pol',
                            metavar='POL',
                            type=str,
                            default='.*',
                            help='Polarization: HH or VV')

        parser.add_argument('--pass',
                            metavar='PASS',
                            type=str,
                            default='.*',
                            help='Satellite pass: ascending or descending')

    def handle(self, *args, **options):
        sep = ' | '

        geometry = self.geometry_from_options(extent=options['extent'],
                                              geojson=options['geojson'])

        ds = Dataset.objects.filter(source__platform__short_name='ENVISAT',
                                    polarization__iregex=r'%s' % options['pol'],
                                    sat_pass__iregex=r'%s' % options['pass'],
                                    time_coverage_start__gte=options['start'],
                                    time_coverage_start__lte=options['end'],
                                    geographic_location__geometry__intersects=geometry)


        i = 0
        for el in ds:
            i += 1
            self.stdout.write(self.style.SQL_TABLE(
                '%4d / %d' % (i, len(ds))) + sep + str(el) + sep + el.polarization
                              + sep + el.sat_pass, ending='\n')
            res = DatasetProcessor().reprocess(el)
            if res is True:
                self.stdout.write(self.style.SUCCESS('OK'))
            else:
                self.stdout.write(self.style.ERROR('ERROR'))
