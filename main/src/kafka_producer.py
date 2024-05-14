import json
import time
import pandas as pd
from confluent_kafka import Producer

KAFKA_TOPIC_NAME_CONS = "shopping_topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

data_path = "/home/dautay/Documents/Projects/lean_DE/big_data_final/dataset/customer_shopping_data.csv"


def stream_data_to_kafka(producer, data, messages_per_second):
    df = pd.DataFrame(
        data,
        columns=[
            "invoice_no",
            "customer_id",
            "gender",
            "age",
            "category",
            "quantity",
            "price",
            "payment_method",
            "invoice_date",
            "shopping_mall",
        ],
    )

    for index, row in df.iterrows():
        message = row.to_dict()
        print(message)
        producer.produce(KAFKA_TOPIC_NAME_CONS, json.dumps(message))
        producer.flush()

        time.sleep(1.0 / messages_per_second)


if __name__ == "__main__":
    print("Bắt đầu Producer gửi message lên Kafka")
    print(f"Tên topic: {KAFKA_TOPIC_NAME_CONS}")

    kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS_CONS})

    messages_per_second = 1  # Số lượng message muốn gửi trong 1 giây

    for chunk in pd.read_csv(data_path, chunksize=messages_per_second):
        stream_data_to_kafka(kafka_producer, chunk.values, messages_per_second)

    # Sau khi send hết message lên topic thì kết thúc kafka producer
    print("Kết thúc chương trình")
