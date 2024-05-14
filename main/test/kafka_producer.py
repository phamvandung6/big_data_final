import json
import time
import pandas as pd
from confluent_kafka import Producer

KAFKA_TOPIC_NAME_CONS = "orderstopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"


def stream_data_to_kafka(producer, data):
    df = pd.DataFrame(
        data,
        columns=[
            "order_id",
            "created_at",
            "discount",
            "product_id",
            "quantity",
            "subtotal",
            "tax",
            "total",
            "customer_id",
        ],
    )
    for index, row in df.iterrows():
        message = row.to_dict()
        print(message)
        producer.produce(KAFKA_TOPIC_NAME_CONS, json.dumps(message))
        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    print("Kafka Producer Application Started...")

    kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS_CONS})

    file_path = (
        "/home/dautay/Documents/Projects/lean_DE/big_data_final/dataset/orders.csv"
    )

    chunk_size = 1  # Số lượng bản ghi bạn muốn stream mỗi lần

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        stream_data_to_kafka(kafka_producer, chunk.values)

    print("Kafka Producer Application Completed.")
