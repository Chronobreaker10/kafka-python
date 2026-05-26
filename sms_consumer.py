import asyncio

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError, CommitFailedError
import logging

logger = logging.getLogger(__name__)

async def process_message():
    sms_consumer = AIOKafkaConsumer(
        'notifications',
        bootstrap_servers='localhost:9092',
        group_id="sms-group",
        # Отключаем автокоммит, важно отправить SMS пользователю
        enable_auto_commit=False
    )
    await sms_consumer.start()
    try:
        async for message in sms_consumer:
            try:
                print(f"#{message.offset} получено уведомление: {message.value.decode("utf-8")}")
                await asyncio.sleep(0.5)
                print(1 / 0)
                await sms_consumer.commit()
                print(f"Уведомление {message.value.decode("utf-8")} отправлено по SMS!")
            except CommitFailedError:
                logger.error("Commit failed: partition revoked. Giving up.")
                break
            except KafkaError as e:
                logger.error(f"Kafka error: {e}")
            except Exception as e:
                logger.error(f"Internal Error: {e}")
                # await send_to_dlq(msg, topic='my_dlq_topic')
                # await sms_consumer.commit()
    finally:
        await sms_consumer.stop()


if __name__ == '__main__':
    asyncio.run(process_message())
