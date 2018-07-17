from django.db import models

from geospaas.catalog.models import Dataset as CatalogDataset

from sar_doppler.managers import DatasetManager


class Dataset(CatalogDataset):

    polarization = models.CharField(null=True, max_length=20, default='')
    sat_pass = models.CharField(null=True, max_length=20, default='',
                                verbose_name='Satellite orbit pass')

    objects = DatasetManager()
