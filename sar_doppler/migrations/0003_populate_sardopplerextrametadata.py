# -*- coding: utf-8 -*-
# Generated by Django 1.11.10 on 2019-06-13 09:24
from __future__ import unicode_literals

from django.db import migrations

from nansat.nansat import Nansat

from geospaas.utils.utils import nansat_filename

def add_polarization(apps, schema_editor):
    ds_model = apps.get_model('sar_doppler', 'dataset')
    extra_model = apps.get_model('sar_doppler', 'sardopplerextrametadata')
    for ds in ds_model.objects.filter(dataseturi__uri__endswith='.gsar'):
        if ds.sardopplerextrametadata_set.all():
            # This should only happen if the migration is interrupted
            # No point in adding polarization if it was already added...
            continue
        n = Nansat(nansat_filename(ds.dataseturi_set.get(uri__endswith='.gsar').uri))
        # Store the polarization and associate the dataset
        extra = extra_model(dataset=ds,
                    polarization=n.get_metadata('polarization'))
        extra.save()
        ds.sardopplerextrametadata_set.add(extra)


class Migration(migrations.Migration):

    dependencies = [
        ('sar_doppler', '0002_auto_20190613_0805'),
    ]

    operations = [
        migrations.RunPython(add_polarization, reverse_code=migrations.RunPython.noop),
    ]
