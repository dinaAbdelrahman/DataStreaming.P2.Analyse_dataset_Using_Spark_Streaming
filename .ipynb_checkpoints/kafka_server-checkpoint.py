import producer_server

##file is in root of the script local directory


def run_kafka_server():
    # TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="sf-streamed-data",
        bootstrap_servers="localhost:9092",
        client_id="client1",
        api_version=(0, 10, 1)
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
