#!/usr/bin/env python3
import sys
import csv

reader = csv.DictReader(sys.stdin)

for row in reader:
    service = row.get("service_name", "").strip()
    endpoint = row.get("endpoint", "").strip()
    response_time = row.get("response_time_ms", "").strip()

    try:
        response_time = int(response_time)
    except ValueError:
        continue

    if service and endpoint and response_time > 800:
        print(f"{service},{endpoint}\t1")
