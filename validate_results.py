#!/usr/bin/env python3
import csv
from collections import Counter


DATA_FILE = "data/cloud_service_logs.csv"
HADOOP_REQUEST_COUNT = "outputs/hadoop_request_count_by_service.txt"
RAY_METRICS = "outputs/ray_service_metrics.txt"
OUTPUT_FILE = "evidence/validation_results.txt"


def read_hadoop_counts(path):
    result = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            service, count = line.strip().split("\t")
            result[service] = int(count)
    return result


def read_ray_totals(path):
    result = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result[row["service_name"]] = int(row["total_requests"])
    return result


def main():
    python_counts = Counter()

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            python_counts[row["service_name"].strip()] += 1

    hadoop_counts = read_hadoop_counts(HADOOP_REQUEST_COUNT)
    ray_totals = read_ray_totals(RAY_METRICS)

    services = sorted(set(python_counts) | set(hadoop_counts) | set(ray_totals))

    passed = True
    lines = []
    lines.append("Validation results")
    lines.append("")
    lines.append("Check: Python independent counts vs Hadoop request counts vs Ray total_requests")
    lines.append("")

    for service in services:
        py_count = python_counts.get(service, 0)
        hadoop_count = hadoop_counts.get(service, 0)
        ray_count = ray_totals.get(service, 0)
        match = py_count == hadoop_count == ray_count

        if not match:
            passed = False

        lines.append(
            f"{service}: python={py_count}, hadoop={hadoop_count}, ray={ray_count}, match={match}"
        )

    lines.append("")
    lines.append(f"Overall validation passed: {passed}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
