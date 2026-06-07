import asyncio
import json
import logging
import string
from random import choices
from datetime import datetime
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError


logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, bootstrap_servers):
        self.producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)

    async def __aenter__(self):
        if self.producer is not None:
            await self.producer.start()
        return self.producer

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.producer is not None:
            await self.producer.stop()


async def send():
    topic = 'notifications'
    i = 0

    async with Producer(bootstrap_servers='localhost:9092') as producer:
        try:
            while True:
                # msg_bytes = f'{message} #{i}'.encode('utf-8')
                msg_obj = {
                    "id": i,
                    "name": "".join(choices(string.ascii_letters, k=10)),
                    "timestamp": datetime.now().isoformat(timespec="seconds")
                }
                # await producer.send_and_wait(topic, msg_bytes)
                await producer.send_and_wait(topic, json.dumps(msg_obj).encode("utf-8"))
                # Можем явно указать ключ (сообщения с одинаковым ключом будут отправлены в одну партицию)
                # await producer.send_and_wait(topic, msg_bytes, key='email'.encode('utf-8'))
                # Можем явно указать номер партиции, сообщения будут отправляться только связанному консьюмеру
                # await producer.send_and_wait(topic, msg_bytes, partition=1)
                i += 1
                print(f'Sent message {msg_obj} in topic {topic}')
                await asyncio.sleep(1)
        except KafkaError as e:
            # Обработка сетевых ошибок, недоступности топика или таймаутов
            logger.error(f"Error sending message: {e}")
        except Exception as e:
            logger.error(f"Internal Error: {e}")

if __name__ == '__main__':
    asyncio.run(send())