import json
import time
import pandas as pd
from datetime import datetime
from settings import TRANSACTIONS_TOPIC, DELAY
from utils import create_producer

_id = 0
# Add your CSV file path here
csv_file_path = 'streaming/data/skenario_1_07_06_2021.csv'

# Read CSV file using pandas
df = pd.read_csv(csv_file_path)
producer = create_producer()

if producer is not None:
    for index, row in df.iterrows():
        # Convert row to dictionary
        X_test = row.to_dict()
        current_time = datetime.utcnow().isoformat()

        record = {"id": _id, "data": X_test, "current_time": current_time}
        record = json.dumps(record).encode("utf-8")

        producer.produce(topic=TRANSACTIONS_TOPIC,value=record)
        producer.flush()
        _id += 1
        time.sleep(DELAY)