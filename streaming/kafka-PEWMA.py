import json
import logging
import pandas as pd
from multiprocessing import Process
from utils import create_producer, create_consumer
from settings import TRANSACTIONS_TOPIC, TRANSACTIONS_CONSUMER_GROUP, ANOMALIES_TOPIC, NUM_PARTITIONS
from probabilisticUniEWMA import probabilisticUniEWMA


def detect():
    consumer = create_consumer(topic=TRANSACTIONS_TOPIC, group_id=TRANSACTIONS_CONSUMER_GROUP)
    producer = create_producer()
    data_buffer = pd.DataFrame()
    window_size = 10
    anom = probabilisticUniEWMA()
    initialized = False

    while True:
        message = consumer.poll(timeout=50)
        if message is None:
            continue
        if message.error():
            logging.error("Consumer error: {}".format(message.error()))
            continue

        record = json.loads(message.value().decode('utf-8'))    
        data = pd.DataFrame([record["data"]['MPSAS']], columns=['MPSAS'])
        data_buffer = data_buffer.append(data)

        if len(data_buffer) >= window_size and not initialized:
            # Initialize the model once with the first batch of data
            anom.init(data_buffer['MPSAS'].values)
            initialized = True  # Model has now been initialized

        elif initialized:
            # Update the model with each new data point
            anom.update(data_buffer.iloc[-1]['MPSAS'])
            score = anom.predict(data_buffer.iloc[-1]['MPSAS'])

            if score < 0.2:  # Adjust this threshold based on your specific application
                record["score"] = score

                _id = str(record["id"])
                record = json.dumps(record).encode("utf-8")

                producer.produce(topic=ANOMALIES_TOPIC, value=record)
                producer.flush()

        if len(data_buffer) >= window_size:
            # Always keep the size of the data buffer at the window size
            data_buffer = data_buffer.iloc[1:]

        # if len(data_buffer) >= window_size:
        #     anom.init(data_buffer['MPSAS'].values)
        #     score = anom.predict(data_buffer.iloc[-1]['MPSAS'])

        #     if score < 0.2:  # Adjust this threshold based on your specific application
        #         record["score"] = score

        #         _id = str(record["id"])
        #         record = json.dumps(record).encode("utf-8")

        #         producer.produce(topic=ANOMALIES_TOPIC, value=record)
        #         producer.flush()

        #         data_buffer = data_buffer.iloc[1:]
        # consumer.commit() # Uncomment to process all messages, not just new ones

    consumer.close()


# One consumer per partition
def main():
    for _ in range(NUM_PARTITIONS):
        p = Process(target=detect)
        p.start()


if __name__ == '__main__':
    main()
