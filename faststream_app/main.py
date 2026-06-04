from fastapi import FastAPI
from faststream.kafka.fastapi.fastapi import KafkaRouter
from models import MessageCreate, Message

app = FastAPI()
# http://127.0.0.1:8000/messages/asyncapi
router = KafkaRouter(bootstrap_servers=['localhost:9092'], prefix="/messages")
publisher = router.publisher("messages-topic")

@router.post("/")
async def send_message(message: MessageCreate):
    await publisher.publish(Message(**message.model_dump()))
    return {"message": "Сообщение успешно отправлено"}


@router.subscriber("messages-topic")
async def receive_message(message: Message):
    print(f"Сообщение успешно обработано: {message}")


app.include_router(router)
