import asyncio

from aiokafka import AIOKafkaProducer


async def send():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'events'
    message = "System event!"
    await producer.start()
    i = 0

    try:
        while True:
            msg_bytes = f'{message} #{i}'.encode('utf-8')
            await producer.send_and_wait(topic, msg_bytes)
            i += 1
            print(f'Sent event {i}')
            await asyncio.sleep(1)
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(send())