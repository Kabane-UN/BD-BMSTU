#!/usr/bin/env python3
import sys
from collections import defaultdict

url_stats = defaultdict(lambda: {'statuses': defaultdict(int), 'days': defaultdict(int), 'total': 0})

for line in sys.stdin:
    try:
        url, status, day = line.strip().split('\t')
        url_stats[url]['total'] += 1
        url_stats[url]['statuses'][status] += 1
        url_stats[url]['days'][day] += 1
    except:
        continue

# Отбираем ТОП-10
top_10 = sorted(url_stats.items(), key=lambda x: x[1]['total'], reverse=True)[:10]

for url, data in top_10:
    total = data['total']
    
    # 1. Вывод распределения кодов
    status_report = []
    for s in ['200', '404', '500']:
        count = data['statuses'][s]
        percent = (count / total) * 100
        status_report.append(f"статус {s} - всего {count} запросов, {percent:.2f}%")
    
    # 2. Расчет аномалий (Среднее и Сигма)
    counts = list(data['days'].values())
    if len(counts) > 0:
        mean = sum(counts) / len(counts)
        variance = sum((x - mean) ** 2 for x in counts) / len(counts)
        std_dev = variance ** 0.5
        
        anomalies_high = []
        anomalies_low = []
        
        for day, count in data['days'].items():
            if count >= mean + 2 * std_dev:
                anomalies_high.append(f"{day}({count})")
            elif count <= mean - 2 * std_dev:
                anomalies_low.append(f"{day}({count})")

        # Итоговый вывод для URL
        print(f"URL: {url}")
        print(f"  Всего запросов: {total}")
        print(f"  Распределение: {', '.join(status_report)}")
        print(f"  Аномально много: {', '.join(anomalies_high) if anomalies_high else 'нет'}")
        print(f"  Аномально мало: {', '.join(anomalies_low) if anomalies_low else 'нет'}")
        print("-" * 30)