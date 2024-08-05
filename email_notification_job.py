# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import datetime

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

PATH_PREFIX = "/dbfs/mnt/"
INITIAL_LOAD = True

table_name = "bronze_alwayson.cc_landing_upload_audit"
table_path = bronze_root + "bronze_schema/landing_upload_audit"

blob_container_list = ["analyticsiq", "transunion", "gfk", "mrisimmons"]


def get_path(folder_name: str) -> str:
    return os.path.join(PATH_PREFIX, folder_name)


def get_files_in_path(folder_name: str):
    for dirpath, dirs, files in os.walk(folder_name):
        for filename in files:
            fname = os.path.join(dirpath, filename)
            yield fname


def get_files_in_container(blob_container_list, get_path_func, get_files_in_path_func):
    for blob_container in blob_container_list:
        path: str = get_path_func(blob_container)
        for file_name in get_files_in_path_func(path):
            yield file_name


def get_epoc(p: str) -> int:
    return datetime.datetime.fromtimestamp(int(os.path.getmtime(p)))


def get_all_files(blob_container_list):
    for x in get_files_in_container(blob_container_list, get_path, get_files_in_path):
        print(x)
        yield {"path": x, "modify_time": get_epoc(x)}


if INITIAL_LOAD:
    df = spark.createDataFrame([x for x in get_all_files(blob_container_list)])
    spark.sql(f"""drop table if exists {table_name} """)
    df.write.format("delta").option("compression", "snappy").option(
        "path", table_path
    ).partitionBy("path").saveAsTable(table_name)

else:
    df = spark.createDataFrame([x for x in get_all_files(blob_container_list)])
    existing_df = spark.sql(f"""Select * from {table_name}""")
    delta = (
        df.alias("a")
        .join(
            existing_df.alias("b"),
            (F.col("a.path") == F.col("b.path")),
            "leftanti",
        )
        .select("a.path", "a.modify_time")
    )
    if delta and delta.count() > 0:
        delta.write.mode("append").save(table_path)

        files_list =  [ x["path"] for x in delta.collect()]

        # email this out to the team.

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.cc_landing_upload_audit
