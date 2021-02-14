import asyncio
from confluent_kafka import Consumer
from confluent_kafka import AdminClient
import sys 

BROKER_URL= "PLAINTEXT://localhost:9092"

topic_name="sf-streamed-data"

def topic_exist(Client, topic_name):
    "checking if topic is available"
    topic_metadata = client.list_topics(timeout=5)
    return topic_name in set(t.topic in t in iter(topic_metadata.topics.values()))


def main():
    client = AdminClient({"bootstrap.servers":BROKER_URL})
    exists = topic_exist(Client, topic_name)
    
    if exists:
        print(f"topic {topic_name} is existing")
    else:
        sys.exit("topic does not exists")
        
    try:
        asyncio.run(consume(topic_name))
    except KeyboardInterrupt as e:
        print("Shuting down")

async def consume(topic_name):
    c=consumer({"bootstrap.servers":BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("No message was received")
        elif message.error is not None:
            print(f"error from consumer: {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(2)


if __name__ == "__main__":
    main()