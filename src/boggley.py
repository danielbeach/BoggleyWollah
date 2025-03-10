import logging
import boto3
from deltalake import DeltaTable
import daft


class BoggleyWollah:
    def __init__(self, 
                 lake_house_type: str,
                 s3_bucket: str,
                 s3_keys: list,
                 unity: bool = False
                 ) -> None:
        self._name = "Boggley Wollah"
        self._lake_house_type = lake_house_type
        self.unity = unity
        self._s3_bucket = s3_bucket
        self._s3_keys = s3_keys
        self.s3_client = boto3.client("s3")
        self.table_files = {}
        self.table_metrics = {}

        self.init_checks()

    def init_checks(self) -> str:
        if self._lake_house_type not in ["delta", "iceberg"]:
            raise Exception("Invalid lake house type, must be either 'delta' or 'iceberg'")
        if not self._s3_bucket:
            raise Exception("S3 bucket name cannot be empty")
        if self._s3_keys == []:
            raise Exception("S3 keys cannot be empty") 

    def pull_all_s3_files(self, table) -> None:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self._s3_bucket, Prefix=table):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj["Key"]
                size = obj["Size"] / (1024 * 1024)
                last_modified = obj["LastModified"]
                files.append({"Key": key, "size": size, "modified": last_modified})
        return files
        
    def calculate_table_metrics(self, table, table_files: list) -> dict:
        ## Tables smaller than 2.56 TB, the autotuned target file size is 256 MB
        total_size = 0
        total_files = 0
        parquet_files = []
        for file in table_files:
            if '.parquet' in file["Key"]:
                total_size += file["size"]
                totl_size_rounded = round(total_size, 2)
                total_files += 1
                parquet_files.append(self.strip_down_parquet_location(file["Key"]))

        average_file_size = round(totl_size_rounded/total_files,2)
        self.table_metrics = {table : {"total_size_mb": totl_size_rounded, "total_parquet_files": total_files, "average_file_size_mb": average_file_size}}
        logging.info(f"Table metrics: {self.table_metrics[table]}")
        return parquet_files

    def strip_down_parquet_location(self, file):
        last_slash_index = file.rfind("/")
        result = file[last_slash_index + 1:]
        return result

    def find_dead_files(self, actual_files, valid_files):
        actual_files_only = []
        for file in actual_files:
            if 'chcekpoint' not in file:
                actual_files_only.append(self.strip_down_parquet_location(file))
        dead_files = list(set(actual_files_only) - set(valid_files))
        if dead_files:
            cnt = len(dead_files)
            logging.info(f"Dead files found {cnt} ")
        else:
            logging.info("No dead files found")
        return dead_files


    def check_average_file_size(self, table) -> None:
        avg_size = self.table_metrics[table]["average_file_size_mb"]
        if avg_size > 256:
            logging.info(f"For table {table} Average file size {avg_size} MB is greater than 256 MB, consider increasing the target file size")
        else:
            logging.info(f"For table {table} Average file size {avg_size} MB is less than 256 MB, you should run a compaction job")

    def analyze_tables(self):
        for table in self._s3_keys:
            logging.info(f"Analyzing table: {table}")
            table_info = DeltaTable(f"s3://{self._s3_bucket}/{table}")
            valid_files = table_info.files()
            table_files = self.pull_all_s3_files(table)
            self.table_files[table] = table_files
            parquet_files = self.calculate_table_metrics(table, table_files)
            self.check_average_file_size(table)
            _ = self.find_dead_files(parquet_files, valid_files)
            


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    boggley = BoggleyWollah("delta", "confessions-of-a-data-guy", 
                            ["picklebob/__unitystorage/catalogs/4c1eb96a-264f-4a0d-bc1a-c80d9fbcdefa/tables/688f96c3-a44d-409e-96db-2a3d075cf7bc/",
                             "picklebob/__unitystorage/catalogs/4c1eb96a-264f-4a0d-bc1a-c80d9fbcdefa/tables/1ee88238-1197-4167-86d5-ec5c86cb8e59"])
    boggley.analyze_tables()
