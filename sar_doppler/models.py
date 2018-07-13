from django.db import models

from geospaas.catalog.models import Dataset as CatalogDataset

from sar_doppler.managers import DatasetManager


class Dataset(CatalogDataset):

    objects = DatasetManager()

    class Meta:
        proxy = True

