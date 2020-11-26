from django.conf import settings

import json
from random import uniform, randint

def get_data(id):
    lower_limit = settings.LOWER_DETECTOR_DATA_LIMIT
    higher_limit = settings.HIGHER_DETECTOR_DATA_LIMIT
    d = {}
    if randint(0, 50) > 49 :
        lower_limit = higher_limit * 5
        higher_limit *= 9.9
        d['anomaly'] = True
    d.update({
        'id': id,
        'temp': round(uniform(lower_limit, higher_limit), 2),
        'Co2': round(uniform(lower_limit, higher_limit), 2),
        'humidity': round(uniform(lower_limit, higher_limit), 2),
        'lightning': round(uniform(lower_limit, higher_limit), 2),
        'pH': round(uniform(lower_limit, higher_limit), 2),
    })
    return json.dumps(d)