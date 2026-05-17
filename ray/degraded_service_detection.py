#!/usr/bin/env python3
import csv
import time
import platform
from collections import defaultdict

import ray


INPUT_FILE = "data/cloud_service_logs.csv"
OUTPUT_FILE = "outputs/ray_degraded_services.txt"
DETAIL_OUTPUT_FILE = "outputs/ray_service_metrics.txt"
EVIDENCE_FILE = "evidence/ray_environment_and_runtime.txt"
CHUNK_SIZE = 5000


@ray.remote
def process_chunk(rows):
    stats = defaultdict(lambda: {
        "total": 0,
        "slow": 0,
        "server_errors": 0,
        "timeouts": 0
    })

    for row in rows:
        service = row.get("service_name", "").strip()
        if not service:
            continue

        try:
            status_code = int(row.get("status_code", "0"))
            response_time_ms = int(row.get("response_time_ms", "0"))
        except ValueError:
            continue

        error_type = row.get("error_type", "").strip()

        stats[service]["total"] += 1

        if response_time_ms > 800:
            stats[service]["slow"] += 1

        if status_code >= 500:
            stats[service]["server_errors"] += 1

        if error_type == "Timeout":
            stats[service]["timeouts"] += 1

    return dict(stats)


def read_csv_chunks(path, chunk_size):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        chunk = []

        for row in reader:
            chunk.append(row)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk


def merge_stats(partial_results):
    merged = defaultdict(lambda: {
        "total": 0,
        "slow": 0,
        "server_errors": 0,
        "timeouts": 0
    })

    for partial in partial_results:
        for service, stats in partial.items():
            merged[service]["total"] += stats["total"]
            merged[service]["slow"] += stats["slow"]
            merged[service]["server_errors"] += stats["server_errors"]
            merged[service]["timeouts"] += stats["timeouts"]

    return dict(merged)


def detect_degraded_services(stats_by_service):
    results = []

    for service, stats in sorted(stats_by_service.items()):
        total = stats["total"]
        slow_rate = stats["slow"] / total if total else 0
        server_error_rate = stats["server_errors"] / total if total else 0
        timeouts = stats["timeouts"]

        if slow_rate > 0.20:
            results.append((service, "high slow request rate"))

        if server_error_rate > 0.10:
            results.append((service, "high server error rate"))

        if timeouts >= 5:
            results.append((service, "repeated timeout errors"))

    return results


def write_service_metrics(stats_by_service):
    with open(DETAIL_OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("service_name,total_requests,slow_requests,slow_rate,server_errors,server_error_rate,timeout_errors\n")

        for service, stats in sorted(stats_by_service.items()):
            total = stats["total"]
            slow_rate = stats["slow"] / total if total else 0
            server_error_rate = stats["server_errors"] / total if total else 0

            f.write(
                f"{service},{total},{stats['slow']},{slow_rate:.4f},"
                f"{stats['server_errors']},{server_error_rate:.4f},{stats['timeouts']}\n"
            )


def main():
    start = time.time()

    ray.init()

    chunks = list(read_csv_chunks(INPUT_FILE, CHUNK_SIZE))
    futures = [process_chunk.remote(chunk) for chunk in chunks]
    partial_results = ray.get(futures)

    merged = merge_stats(partial_results)
    degraded_services = detect_degraded_services(merged)

    write_service_metrics(merged)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("service_name,reason\n")
        for service, reason in degraded_services:
            f.write(f"{service},{reason}\n")

    end = time.time()

    with open(EVIDENCE_FILE, "w", encoding="utf-8") as f:
        f.write("Ray degraded-service detection evidence\n\n")
        f.write(f"Runtime seconds: {end - start:.4f}\n")
        f.write(f"Python version: {platform.python_version()}\n")
        f.write(f"Platform: {platform.platform()}\n")
        f.write(f"Ray version: {ray.__version__}\n")
        f.write(f"Ray mode: local mode on AWS EC2\n")
        f.write(f"Input file: {INPUT_FILE}\n")
        f.write(f"Chunk size: {CHUNK_SIZE}\n")
        f.write(f"Number of chunks: {len(chunks)}\n")
        f.write(f"Number of Ray remote tasks: {len(futures)}\n")
        f.write(f"Ray available resources: {ray.available_resources()}\n")
        f.write("\nDegraded service output file:\n")
        f.write(f"{OUTPUT_FILE}\n")
        f.write("\nDetailed service metrics file:\n")
        f.write(f"{DETAIL_OUTPUT_FILE}\n")

    ray.shutdown()


if __name__ == "__main__":
    main()
