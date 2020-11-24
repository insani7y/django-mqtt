import logging
import asyncio
from hbmqtt.broker import Broker
from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_1 

logger = logging.getLogger(__name__)

config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': 'localhost:9999',    # 0.0.0.0:1883
            'max-connections': 50000,
        },
    },
    'sys_interval': 500,
    'topic-check': {
        'enabled': True,
        'plugins': ['topic_taboo'],
    }
}

broker = Broker(config)

async def startBroker():
    await broker.start()

async def brokerGetMessage():
    client = MQTTClient()
    await client.connect('mqtt://localhost:9999/')
    await client.subscribe([
        ('LINTANGtopic/test', QOS_1)
    ])
    logger.info('Subscribed!')
    try: 
        for i in range(1, 100):
            message = await client.deliver_message()
            packet = message.publish_packet
            print(packet.payload.data.decode('utf-8'))
    except ClientException as ce:
        logger.error(f'Client exception {ce}')


if __name__ == '__main__':
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(startBroker())
    asyncio.get_event_loop().run_until_complete(brokerGetMessage())
    asyncio.get_event_loop().run_forever()