## BoggleyWollah
A Python package to help understand the health of your `LakeHouse`.


### What does it do?
`BoggleyWollah` is made to help you ...
- Understand the size(s) of the files in your `Lake House` ... are they too big or small?
- Understand if you have "dead files" (unreferenced files).
- Data skew via inspecting your partitioning or clustering strategy.

### Lake House Support
The goal is to support both `Delta Lake` and `Iceberg` tables in `s3`.

### Usage
```
logging.basicConfig(level=logging.INFO)
boggley = BoggleyWollah("delta", "confessions-of-a-data-guy", ["some/table/some/location/"])
boggley.analyze_tables()

INFO:botocore.credentials:Found credentials in environment variables.
INFO:root:Analyzing table: some/table/some/location
INFO:root:Table metrics: {'total_size_mb': 1238.24, 'total_parquet_files': 176, 'average_file_size_mb': 7.04}
INFO:root:For table some/table/some/location Average file size 7.04 MB is less than 256 MB, you should run a compaction job
INFO:root:Dead files found 175: 
 ```