#!/usr/bin/env python3
import sys
import csv

reader = csv.DictReader(sys.stdin)

for row in reader:
    service = row.get("service_name", "").strip()
    status_code = row.get("status_code", "").strip()

    try:
        status_code = int(status_code)
    except ValueError:
        continue

    if service and status_code >= 500:
        print(f"{service}\t1")
