import asyncio
import logging
from random import randint

from aiokafka import AIOKafkaProducer

from watering_app.config import SENSOR_TOPIC_NAME
from watering_app.models import HumidityReading

logger = logging.getLogger(__name__)


async def send_humidity():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()
    sensor_id = 1

    try:
        while True:
            sensor_data = HumidityReading(id=sensor_id, value=randint(0, 100))
            await producer.send_and_wait(SENSOR_TOPIC_NAME, sensor_data.model_dump_json().encode('utf-8'))
            logger.info(
                f"Отправлено значение из датчика влажности: {sensor_data.value} с id={sensor_id} "
                f"в {sensor_data.timestamp.strftime('%Y-%m-%d %H:%M:%S')}."
            )
            await asyncio.sleep(5)
            sensor_id += 1
    except Exception as e:
        logger.error(f"При отправке значения с датчика влажности произошла ошибка: {e}")
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(send_humidity())
