import os
from os.path import join, dirname

from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

DELAY = 1
NUM_PARTITIONS = 1
KAFKA_BROKER = "localhost:9092"
TRANSACTIONS_TOPIC = "transactions-1"
TRANSACTIONS_CONSUMER_GROUP = "transactions-1"
ANOMALIES_TOPIC = "anomalies-1"
ANOMALIES_CONSUMER_GROUP = "anomalies-1"
