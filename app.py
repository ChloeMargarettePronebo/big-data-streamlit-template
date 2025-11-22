import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import time

TOPIC = "streaming-data"
BOOTSTRAP_SERVERS = "localhost:9093"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

st.title("Streaming Data Dashboard")
st.write("Real-time weather streaming view")

df = pd.DataFrame(columns=["temperature", "windspeed", "winddirection", "timestamp"])
table = st.empty()

for message in consumer:
    data = message.value
    df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    table.dataframe(df.tail(10))  # show last 10 messages
    time.sleep(0.1)
