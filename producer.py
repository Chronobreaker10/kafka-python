import asyncio

from aiokafka import AIOKafkaProducer


async def send():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'notifications'
    message = "Hello, Kafka!"
    await producer.start()
    i = 0

    try:
        while True:
            msg_bytes = f'{message} #{i}'.encode('utf-8')
            await producer.send_and_wait(topic, msg_bytes)
            # Можем явно указать ключ (сообщения с одинаковым ключом будут отправлены в одну партицию)
            # await producer.send_and_wait(topic, msg_bytes, key='email'.encode('utf-8'))
            # Можем явно указать номер партиции, сообщения будут отправляться только связанному консьюмеру
            # await producer.send_and_wait(topic, msg_bytes, partition=1)
            i += 1
            print(f'Sent message {i}')
            await asyncio.sleep(1)
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(send())