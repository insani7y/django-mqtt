from django.shortcuts import get_object_or_404
import django

from asgiref.sync import sync_to_async

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_mqtt.settings')
django.setup()

from detector.models import Detector, DetectorData

@sync_to_async
def split_json_and_create_data(data):
    data['detector'] = get_object_or_404(Detector, id=data.pop('id'))
    DetectorData.objects.create(**data)