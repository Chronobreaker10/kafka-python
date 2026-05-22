import asyncio

from aiokafka import AIOKafkaConsumer


async def process_message():
    consumer = AIOKafkaConsumer('test', bootstrap_servers='localhost:9092', group_id="my-group")
    await consumer.start()
    try:
        async for message in consumer:
            print(f'Получение сообщения #{message.offset}: {message.value.decode("utf-8")}')
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(process_message())