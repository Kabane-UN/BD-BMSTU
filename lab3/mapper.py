#!/usr/bin/env python3
import sys
import io
import re

# Игнорируем ошибки кодировки
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, errors='ignore')

# Регулярка для логов NASA: IP - - [Дата] "GET URL ..." Status Size
log_re = re.compile(r'^\S+ - - \[(.*?)\] ".*?\s(\S+)\s.*?" (\d{3})')

for line in sys.stdin:
    line = line.strip()
    match = log_re.match(line)
    if match:
        date_str, url, status = match.groups()
        clean_url = url.lower()
        if not (clean_url.endswith('.html') or clean_url.endswith('.htm')):
            continue
        # Дата в логах: 01/Aug/1995:00:00:01
        # Нам нужен только день для анализа аномалий
        day = date_str.split(':')[0] 
        # Вывод: URL <tab> статус <tab> день
        print(f"{url}\t{status}\t{day}")