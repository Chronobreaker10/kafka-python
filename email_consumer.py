import asyncio

from aiokafka import AIOKafkaConsumer


async def process_message():
    email_consumer = AIOKafkaConsumer(
        'notifications',
        bootstrap_servers='localhost:9092',
        group_id="email-group"
    )
    await email_consumer.start()
    try:
        async for message in email_consumer:
            print(f"#{message.offset} получено уведомление: {message.value.decode("utf-8")}")
            if message.key is not None:
                print("по ключу " + message.key.decode("utf-8"))
            await asyncio.sleep(0.5)
            print(f"Уведомление {message.value.decode("utf-8")} отправлено на почту!")
    finally:
        await email_consumer.stop()


if __name__ == '__main__':
    asyncio.run(process_message())
