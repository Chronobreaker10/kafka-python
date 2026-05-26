import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic


async def create_topic():
    client = AIOKafkaAdminClient(bootstrap_servers='localhost:9092')
    try:
        await client.start()
        await client.create_topics([NewTopic(name="notifications", num_partitions=2, replication_factor=1)])
    finally:
        await client.close()

async def delete_topic():
    client = AIOKafkaAdminClient(bootstrap_servers='localhost:9092')
    try:
        await client.start()
        await client.delete_topics(["notifications"])
    finally:
        await client.close()


if __name__ == '__main__':
    asyncio.run(create_topic())
