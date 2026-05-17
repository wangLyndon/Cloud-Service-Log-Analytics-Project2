#!/usr/bin/env python3
import sys
import csv

reader = csv.DictReader(sys.stdin)

for row in reader:
    service = row.get("service_name", "").strip()
    if service:
        print(f"{service}\t1")
