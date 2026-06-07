from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
import logging
from models import Order, Response, EmailMessage

app = FastAPI()
logger = logging.getLogger(__name__)


async def send_to_broker(message: EmailMessage):
    producer = AIOKafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = "emails"

    await producer.start()
    msg_bytes = message.model_dump_json().encode('utf-8')

    try:
        await producer.send_and_wait(topic, msg_bytes)
    except Exception as err:
        logger.error("При отправке email произошла ошибка: ", err)
        raise
    finally:
        await producer.stop()


@app.post(
    "/orders",
    response_model=Response, response_model_exclude_none=True,)
async def send_order(order: Order):
    total = sum(item.price * item.quantity for item in order.items)
    body = f"Ваш заказ в составе: {len(order.items)} позиций на общую сумму {total} руб. сформирован и ожидает оплаты"
    message = EmailMessage(email=order.user.email, user_name=order.user.name, subject=f"Заказ {order.id}", body=body)
    await send_to_broker(message)
    return Response(msg="Заказ обработан, более подробную информацию отправили на Ваш email")
