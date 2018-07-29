from sardoppler.sardoppler import Doppler
import os
from geospaas.utils import nansat_filename, product_path
from geospaas.catalog.models import DatasetURI


class DatasetProcessor(object):
    NUM_SUBSWATS = 5
    DOMAIN = 'file://localhost'
    WKV_NAME = {
        'dc_anomaly': 'anomaly_of_surface_backwards_doppler_centroid_frequency_shift_of_radar_wave',
        'dc_velocity': 'surface_backwards_doppler_frequency_shift_of_radar_wave_due_to_surface_velocity'
    }

    def reprocess(self, ds):
        for i in range(self.NUM_SUBSWATS):
            file_uri = ds.dataseturi_set.first().uri
            try:
                self.generate_product(nansat_filename(file_uri), i, ds)
            except Exception as ex:
                print(ex)
                return False
        return True

    def generate_product(self, uri, swath_num, dataset):
        swath_data = Doppler(uri, subswath=swath_num)
        swath_data.add_band(array=swath_data.anomaly(),
                            parameters={'wkv': self.WKV_NAME['dc_anomaly']})
        swath_data.add_band(array=swath_data.geophysical_doppler_shift(),
                            parameters={'wkv': self.WKV_NAME['dc_velocity']})

        self.export(swath_data, swath_num, dataset)

    def export(self, swath_data, swath_num, dataset):
        ppath = self.get_product_path(swath_data.filename)
        file_dst = DatasetProcessor.assemble_filename(ppath, swath_data.filename, swath_num)
        # print('Exporting: %s' % file_dst)
        swath_data.set_metadata(key='Originating file', value=swath_data.filename)
        swath_data.export(filename=file_dst)
        ncuri = os.path.join(self.DOMAIN, file_dst)
        new_uri, created = DatasetURI.objects.get_or_create(uri=ncuri, dataset=dataset)

    @staticmethod
    def assemble_filename(ppath, origin, swath_num):
        basename = os.path.basename(origin).split('.')[0]
        return '%s/%ssubswath%d.nc' % (ppath, basename, swath_num)

    def get_product_path(self, origin):
        mm = self.__module__.split('.')
        module = '%s.%s' % (mm[0], mm[1])
        # local uri path for visualizations
        ppath = product_path(module, origin)
        return ppath
