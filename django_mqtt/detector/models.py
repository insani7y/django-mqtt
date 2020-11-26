from django.db import models
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404

import paho.mqtt.client as mqtt

from .service import get_data

class Detector(models.Model):
    '''Датчик'''
    x = models.DecimalField('Координата x', max_digits=9, decimal_places=6)
    y = models.DecimalField('Координата y', max_digits=9, decimal_places=6)
    user = models.ForeignKey(
        User, 
        verbose_name='Владелец', 
        null=True,
        on_delete=models.SET_NULL, 
        related_name='my_detectors'
    )

    class Meta:
        verbose_name = 'Датчик'
        verbose_name_plural = 'Датчики'

    def trigger(self):
        client = mqtt.Client()
        client.connect('localhost', 1883)
        data = get_data(self.id)
        client.publish('data', data)
        client.disconnect()


class DetectorData(models.Model):
    '''Данные датчика'''
    detector = models.ForeignKey(
        Detector, 
        verbose_name='Привязанный датчик',
        on_delete=models.DO_NOTHING, 
        related_name='data'
    )
    temp = models.DecimalField('Температура', max_digits=4, decimal_places=2)
    Co2 = models.DecimalField('Содержание углекислого газа', max_digits=4, decimal_places=2)
    humidity = models.DecimalField('Влажность', max_digits=4, decimal_places=2)
    lightning = models.DecimalField('Освещенность', max_digits=4, decimal_places=2)
    pH = models.DecimalField('Кислотность', max_digits=4, decimal_places=2)
    timestamp = models.DateField('Дата сбора данных', auto_now_add=True)
    anomaly = models.BooleanField('Аномалия', default=False)

    def __str__(self):
        return f'{self.anomaly}'

    class Meta:
        verbose_name = 'Даные датчика'
        verbose_name_plural = 'Даные датчиков'