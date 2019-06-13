from django.db import models

from geospaas.catalog.models import Dataset as CatalogDataset

from sar_doppler.managers import DatasetManager

class Dataset(CatalogDataset):

    objects = DatasetManager()

    class Meta:
        proxy = True

class SARDopplerExtraMetadata(models.Model):

     class Meta:
         unique_together = (("dataset", "polarization"))

     dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
     polarization = models.CharField(default='', max_length=100)

