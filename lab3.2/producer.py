import time
import re
import json
from kafka import KafkaProducer
import os

# Конфигурация
TOPIC_NAME = 'nasa_access_logs'
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:29092') # Используем внешний порт из docker-compose
LOG_FILE = 'access.log'    # Укажи свое имя файла

# Регулярка для формата: host - - [date] "method url protocol" status size
LOG_PATTERN = r'(\S+) - - \[(.*?)\] "(.*?) (.*?) (.*?)" (\d+) (\d+|-)'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_logs():
    producer = create_producer()
    print(f"Starting stream to topic: {TOPIC_NAME}...")

    while True: # Цикл для зацикливания файла, как просили в задании
        with open(LOG_FILE, 'r', encoding='latin-1') as f:
            for line in f:
                line = line.strip()
                match = re.match(LOG_PATTERN, line)
                
                if match:
                    # Формируем структуру данных
                    payload = {
                        "host": match.group(1),
                        "timestamp": match.group(2),
                        "method": match.group(3),
                        "url": match.group(4),
                        "protocol": match.group(5),
                        "status": int(match.group(6)),
                        "bytes": 0 if match.group(7) == '-' else int(match.group(7))
                    }
                    
                    # Отправляем в Кафку
                    producer.send(TOPIC_NAME, payload)
                    
                    # Небольшая пауза, чтобы не забить Кафку мгновенно 
                    # и успеть увидеть процесс в UI
                    time.sleep(0.01) 
            
            print("Reached end of file. Restarting...")

if __name__ == "__main__":
    try:
        stream_logs()
    except KeyboardInterrupt:
        print("Stopping producer...")