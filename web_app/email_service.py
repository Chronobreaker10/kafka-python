import asyncio

from aiokafka import AIOKafkaConsumer

from models import EmailMessage


async def consume_messages():
    consumer = AIOKafkaConsumer("emails", bootstrap_servers=["localhost:9092"], group_id="email_senders")

    await consumer.start()

    try:
        async for message in consumer:
            body = message.value.decode("utf-8")
            email_message = EmailMessage.model_validate_json(body)
            print(f"Отправлен email {email_message.email}, тема - {email_message.subject}, тело - {email_message.body}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_messages())
