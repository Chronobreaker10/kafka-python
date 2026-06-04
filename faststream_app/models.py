from datetime import datetime, timezone
from typing import Annotated
from pydantic import BaseModel, Field
from uuid import UUID, uuid4


class MessageBase(BaseModel):
    sender_id: Annotated[int, Field(title="ID отправителя", ge=1)]
    recipient_id: Annotated[int, Field(title="ID получателя", ge=1)]
    subject: Annotated[str, Field(title="Тема сообщения", min_length=1, max_length=128)]
    body: Annotated[str, Field(title="Содержание сообщения", min_length=1, max_length=256)]


class Message(BaseModel):
    id: Annotated[UUID, Field(default_factory=uuid4, title="Идентификатор сообщения")]
    sent_at: Annotated[
        datetime, Field(title="Дата отправки сообщения", default_factory=lambda: datetime.now(timezone.utc))]


class MessageCreate(MessageBase):
    pass
