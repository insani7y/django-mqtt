from django.conf import settings

import os
import logging
import asyncio
import json

from hbmqtt.broker import Broker
from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_1 

from service import split_json_and_create_data

logger = logging.getLogger(__name__)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_mqtt.settings')

broker = Broker(settings.MQTT_CONFIG)

async def startBroker():
    await broker.start()

async def brokerGetMessage():
    client = MQTTClient()
    await client.connect('mqtt://localhost:1883/')
    await client.subscribe([
        ('data', QOS_1)
    ])
    logger.info('Subscribed!')
    try: 
        for i in range(1, 100):
            message = await client.deliver_message()
            packet = message.publish_packet
            data = json.loads(packet.payload.data.decode('utf-8'))
            await split_json_and_create_data(data)
    except ClientException as ce:
        logger.error(f'Client exception {ce}')


if __name__ == '__main__':
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(startBroker())
    asyncio.get_event_loop().run_until_complete(brokerGetMessage())
    asyncio.get_event_loop().run_forever()