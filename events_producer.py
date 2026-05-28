import asyncio
from datetime import datetime

from aiokafka import AIOKafkaProducer
from schemas import Event


async def send():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'events'
    message = "System event!"
    await producer.start()
    i = 1

    try:
        while True:
            # msg_bytes = f'{message} #{i}'.encode('utf-8')
            event = Event(id=i, name=message, timestamp=datetime.now())
            await producer.send_and_wait(topic, event.model_dump_json().encode("utf-8"))
            print(f'Sent event {i}')
            i += 1
            await asyncio.sleep(1)
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(send())