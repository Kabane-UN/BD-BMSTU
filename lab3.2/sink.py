import json
import time
from kafka import KafkaConsumer
import psycopg2
import os

# Ждем немного, чтобы Кафка и Постгрес успели подняться
time.sleep(10)
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:29092')

# Имена хостов берем из docker-compose (kafka и postgres)
consumer = KafkaConsumer(
    'URL_METRICS',
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    group_id='python-sink-group'
)

conn = psycopg2.connect(
    dbname="nasa_analytics",
    user="user",
    password="password",
    host="postgres"
)
cur = conn.cursor()

print("Sink service started. Listening for messages...")

for msg in consumer:
    try:
        # 1. Декодируем значение (JSON)
        data = json.loads(msg.value)
        
        # 2. Достаем URL из ключа (Key)
        # ksqlDB часто дополняет строковый ключ байтами, отсекаем их до первого нулевого байта или декодируем аккуратно
        raw_key = msg.key.decode('utf-8', errors='ignore')
        # Очищаем от непечатных символов, которые ksqlDB добавляет в конец ключа
        url = raw_key.split('\x00')[0].strip()

        print(f"Processing URL: {url}")

        cur.execute("""
            INSERT INTO "URL_METRICS" (url, total_requests, success_count, error_count, success_rate_percent)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET 
                total_requests = EXCLUDED.total_requests,
                success_count = EXCLUDED.success_count,
                error_count = EXCLUDED.error_count,
                success_rate_percent = EXCLUDED.success_rate_percent;
        """, (url, data['TOTAL_REQUESTS'], data['SUCCESS_COUNT'], data['ERROR_COUNT'], data['SUCCESS_RATE_PERCENT']))
        
        conn.commit()
    except Exception as e:
        print(f"Error processing message: {e}")
        # Печатаем само сообщение, чтобы понять структуру при ошибке
        print(f"Failed record: {msg}")
        conn.rollback() 
        print(f"Failed record offset: {msg.offset}")
        