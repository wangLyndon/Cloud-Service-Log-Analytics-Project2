# Mini-Project 2: Cloud Service Log Analytics

## Overview

This repository contains a project for a cloud log analytics mini-project. The work analyses a cloud service log dataset with two approaches:

- Hadoop Streaming MapReduce for service-level counting and slow-endpoint analysis
- Ray for degraded-service detection based on slow responses, server errors, and timeout patterns



## Repository Structure

```text
submission_package/
|-- data/
|   `-- cloud_service_logs.csv
|-- evidence/
|   |-- ec2_s3_download_check.txt
|   |-- hadoop_environment.txt
|   |-- ray_environment_and_runtime.txt
|   `-- validation_results.txt
|-- mapreduce/
|   |-- mapper_request_count.py
|   |-- mapper_server_error_count.py
|   |-- mapper_slow_endpoints.py
|   `-- reducer_sum.py
|-- outputs/
|   |-- hadoop_request_count_by_service.txt
|   |-- hadoop_server_error_count_by_service.txt
|   |-- hadoop_top_10_slow_endpoints.txt
|   |-- ray_degraded_services.txt
|   `-- ray_service_metrics.txt
|-- ray/
|   `-- degraded_service_detection.py
|-- requirements.txt
`-- validate_results.py
```

## Main Contents

- `mapreduce/`: Hadoop Streaming mapper and reducer scripts for request counting, server-error counting, and slow-endpoint aggregation
- `ray/`: Ray-based analysis for degraded-service detection
- `outputs/`: final generated results
- `evidence/`: environment records and validation results
- `validate_results.py`: cross-checks Hadoop request counts, Ray totals, and independent Python counts

## Included Results

The repository includes the final result files directly:

- Hadoop request count by service
- Hadoop server error count by service
- Hadoop top 10 slow endpoints
- Ray degraded service detection results
- Ray per-service metrics

Validation has also been included in `evidence/validation_results.txt`, where the independent Python counts, Hadoop counts, and Ray totals are consistent for all services.

## Environment

- Python dependency: `ray==2.51.2`
- Input dataset: `data/cloud_service_logs.csv`
- Hadoop and Ray execution evidence is recorded in the `evidence/` directory
