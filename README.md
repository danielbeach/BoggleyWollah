# BoggleyWollah

## Overview
`BoggleyWollah` is a Python package designed to help you assess the health of your `LakeHouse` storage in AWS S3. It provides insights into file sizes, detects "dead files" (unreferenced files), and evaluates data skew by inspecting partitioning or clustering strategies.

## Features
- **File Size Analysis**: Determines whether the Parquet files in your Lake House are optimally sized.
- **Dead File Detection**: Identifies unreferenced files that are no longer needed.
- **Partitioning and Clustering Assessment**: Helps identify potential data skew issues.
- **Supports Multiple Lake House Formats**: Compatible with both `Delta Lake` and `Iceberg` tables stored in S3.

## Requirements
- Python 3.7+
- AWS `boto3` for S3 interactions
- `deltalake` library for Delta table analysis
- 'getdaft` library to analyze Lake House tables

## Installation
You can install the required dependencies using:
```sh
pip install boto3 deltalake
```

## Usage
### Basic Example
```python
import logging
from boggleywollah import BoggleyWollah

logging.basicConfig(level=logging.INFO)

boggley = BoggleyWollah(
    lake_house_type="delta",
    s3_bucket="confessions-of-a-data-guy",
    s3_keys=["some/table/some/location/"]
)

boggley.analyze_tables()
```
### Sample Output
```
INFO:botocore.credentials:Found credentials in environment variables.
INFO:root:Analyzing table: some/table/some/location
INFO:root:Table metrics: {'total_size_mb': 1238.24, 'total_parquet_files': 176, 'average_file_size_mb': 7.04}
INFO:root:For table some/table/some/location Average file size 7.04 MB is less than 256 MB, you should run a compaction job
INFO:root:Dead files found 175
```

## API Reference
### `BoggleyWollah` Class
#### `__init__`
```python
BoggleyWollah(lake_house_type: str, s3_bucket: str, s3_keys: list, unity: bool = False)
```
- **lake_house_type**: `"delta"` or `"iceberg"`
- **s3_bucket**: Name of the S3 bucket
- **s3_keys**: List of table prefixes to analyze
- **unity**: Flag for Unity Catalog integration (default: `False`)

#### `analyze_tables()`
- Iterates over the provided tables, calculates file size metrics, detects dead files, and provides optimization suggestions.

#### `pull_all_s3_files(table: str) -> list`
- Fetches all S3 files under a given table prefix.

#### `calculate_table_metrics(table: str, table_files: list) -> dict`
- Computes total size, file count, and average file size for a table.

#### `check_average_file_size(table: str) -> None`
- Suggests compaction if files are too small or a target file size increase if they are too large.

#### `find_dead_files(actual_files: list, valid_files: list) -> list`
- Identifies files present in S3 but not referenced by the Lake House metadata.

## Best Practices
- **Monitor File Sizes**: Ensure files are within optimal size ranges to balance performance and cost.
- **Regular Cleanup**: Use dead file detection to keep storage efficient.
- **Partition & Cluster Wisely**: Avoid small file problems by choosing appropriate partitioning strategies.

## Roadmap
- Add support for additional storage formats (e.g., Hudi)
- Implement deeper insights into partitioning strategies
- Automate compaction recommendations

## License
This project is licensed under the MIT License.

