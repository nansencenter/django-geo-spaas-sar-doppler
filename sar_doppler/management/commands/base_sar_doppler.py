from django.core.management.base import BaseCommand, CommandError
from datetime import datetime


class Command(BaseCommand):

    def add_arguments(self, parser):
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

        # Specify a range of time
        parser.add_argument('--start',
                            metavar='YYYY-MM-DD',
                            type=str,
                            default='1900-01-01',
                            help='Specify start of a time range')

        parser.add_argument('--end',
                            metavar='YYYY-MM-DD',
                            type=str,
                            default='2093-03-11',
                            help='Specify end of a time range')

        # Force processing
        parser.add_argument('--force',
                            action='store_true',
                            help='Force processing ')

        return parser

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

    @staticmethod
    def parse_date(timestamp):
        return datetime.strptime(timestamp, '%Y-%m-%d')
