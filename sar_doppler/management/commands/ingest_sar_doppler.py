''' Processing of SAR Doppler from Norut's GSAR '''
from optparse import make_option

from nansat.tools import GeolocationError

from django.core.management.base import BaseCommand, CommandError

from geospaas.utils import uris_from_args
from geospaas.catalog.models import DatasetURI
from geospaas.catalog.models import Dataset as catalogDataset

from sar_doppler.models import Dataset
from sar_doppler.errors import AlreadyExists
import os


class Command(BaseCommand):
    args = '<filename>'
    # TODO: The description should be updated with new functionality and examples. Artem. 10-07-18
    help = 'Add WS file to catalog archive and make png images for display in Leaflet'

    def add_arguments(self, parser):
        parser.add_argument('gsar_files', nargs='*', type=str)
        # Activate domain
        parser.add_argument('--with-domain',
                            action='store_true',
                            help='Use this parameter if you want to initiate ingesting for a'
                                 'specific domain. Spatial reference (--epsg or --proj4) and '
                                 'extent parameters ((--te or --lle) and (--tr or --ts))'
                                 'must be defined separately')
        # Spatial reference options
        parser.add_argument('--epsg',
                            metavar='EPSG_CODE',
                            type=int,
                            help='Coordinate systems (projected or geographic) can be '
                                 'selected based on their EPSG codes')
        parser.add_argument('--proj4',
                            metavar='\'PROJ4 STRING\'',
                            type=str,
                            help='A PROJ.4 definition string can be used as a coordinate'
                                 ' system. For instance "+proj=utm +zone=11 +datum=WGS84".')
        # Extent options
        parser.add_argument('--tr',
                            metavar=('X', 'Y'),
                            nargs=2,
                            type=float,
                            help='Set target resolution. The values must be expressed in '
                                 'georeferenced units. Both must be positive values')

        parser.add_argument('--te',
                            metavar=('X_MIN', 'Y_MIN', 'X_MAX', 'Y_MAX'),
                            nargs=4,
                            type=float,
                            help='Set georeferenced extents. The values must be expressed '
                                 'in georeferenced units. If not specified, the extent of '
                                 'the output file will be the extent of the vector layers.')
        parser.add_argument('--ts',
                            metavar=('WIDTH', 'HEIGHT'),
                            nargs=2,
                            type=int,
                            help='Set output file size in pixels and lines. '
                                 'Note that --ts cannot be used with --tr. size_x size_y')
        parser.add_argument('--lle',
                            metavar=('LON_MIN', 'LAT_MIN', 'LON_MAX', 'LAT_MAX'),
                            nargs=4,
                            type=float,
                            help='Set domain boundaries. '
                                 'Note that --lle cannot be used with --te')

        # Processing options and features
        parser.add_argument('--reprocess',
                            action='store_true',
                            help='Force reprocessing')

    def handle(self, *args, **options):

        srs, extent = None, None
        if options['with_domain'] is True:
            srs, extent = Command.validate_domain(options)
            self.stdout.write('Domain parameters are valid')

        for non_ingested_uri in uris_from_args(options['gsar_files']):
            self.stdout.write('Ingesting %s ...\n' % non_ingested_uri)
            ds, cr = Dataset.objects.get_or_create(non_ingested_uri, srs, extent, **options)
            if not type(ds) == catalogDataset:
                self.stdout.write('Not found: %s\n' % non_ingested_uri)
            elif ds is None:
                self.stdout.write('Does not intersect with a required domain: %s\n'
                                  % non_ingested_uri)
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

    @staticmethod
    def validate_domain(options):
        # Check Spatial reference options
        srs = Command.check_srs(options)
        if srs is None:
            raise CommandError('Spatial reference was not specified')

        extent = {}
        extent = Command.check_extent_pairs(extent, options, 'lle', 'te')
        if extent is None:
            raise CommandError('--lle or --te was not specified')

        extent = Command.check_extent_pairs(extent, options, 'tr', 'ts')
        if extent is None:
            raise CommandError('--tr or --ts was not specified')

        return srs, extent

    @staticmethod
    def check_srs(options):
        if options['epsg'] is not None and options['proj4'] is not None:
            raise CommandError('Only one Spatial reference system can be used (EPSG or PROJ4)')
        elif options['epsg'] is not None:
            return options['epsg']
        elif options['proj4'] is not None:
            return options['proj4']
        else:
            return None

    @staticmethod
    def check_extent_pairs(extent, options, param1, param2):
        if options[param1] is not None and options[param2] is not None:
            raise CommandError('--%s cannot be used with --%s' % (param1, param2))
        elif options[param1] is not None:
            extent[param1] = options[param1]
            return extent
        elif options[param2] is not None:
            extent[param2] = options[param2]
            return extent
        else:
            return None

