import asyncio
import logging
from random import randint

from aiokafka import AIOKafkaProducer

from watering_app.config import SENSOR_TOPIC_NAME
from watering_app.models import TemperatureReading

logger = logging.getLogger(__name__)


async def send_temperature():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092', request_timeout_ms=5000)
    await producer.start()
    sensor_id = 1

    try:
        while True:
            sensor_data = TemperatureReading(id=sensor_id, value=randint(-10, 40))
            await producer.send_and_wait(SENSOR_TOPIC_NAME, sensor_data.model_dump_json().encode('utf-8'))
            # await producer.flush()
            logger.info(
                f"Отправлено значение из датчика температуры: {sensor_data.value} с id={sensor_id} "
                f"в {sensor_data.timestamp.strftime('%Y-%m-%d %H:%M:%S')}."
            )
            await asyncio.sleep(5)
            sensor_id += 1
    except Exception as e:
        logger.error(f"При отправке значения с датчика температуры произошла ошибка: {e}")
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(send_temperature())
