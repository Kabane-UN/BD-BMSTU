# Используем легковесный образ Python
FROM python:3.14-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем библиотеку для работы с Kafka
RUN pip install --no-cache-dir kafka-python

# Копируем наш скрипт и файл логов в контейнер
# (Убедись, что файл логов лежит в той же папке на хосте)
COPY producer.py .
COPY access.log .

# Запускаем скрипт
CMD ["python", "-u", "producer.py"]