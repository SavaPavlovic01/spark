import requests_oauthlib
import os
import requests
import json
from dotenv import load_dotenv
from kafka import KafkaProducer

url = 'http://127.0.0.1:5000/users.json'

def get_data():
    cnt = 0
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])
    resp = requests.get(url=url, stream=True)
    buffer = ""
    for item in resp.iter_content():
        buffer = buffer + str(item, 'utf-8')
        try:
            json.loads(buffer)
            producer.send("users", buffer.encode())
            buffer = ""
            cnt += 1
            print(cnt)
            
        except:
            continue

if __name__ == "__main__":
    get_data()
    