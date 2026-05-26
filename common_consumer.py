import asyncio

from aiokafka import AIOKafkaConsumer


async def process_message():
    common_consumer = AIOKafkaConsumer(
        'notifications',
        'events',
        bootstrap_servers='localhost:9092',
        group_id="common-group",
        # При первом запуске группы возьмет все сообщения с начала топика
        auto_offset_reset="earliest"
    )
    await common_consumer.start()
    try:
        # common_consumer.subscribe(pattern=r'^events.*') подпишемся на все топики, начинающиеся c events
        async for message in common_consumer:
            print(f"#{message.offset} получено сообщение: {message.value.decode("utf-8")}, topic: {message.topic}")
    finally:
        await common_consumer.stop()


if __name__ == '__main__':
    asyncio.run(process_message())