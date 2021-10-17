from datetime import datetime

import pyspark.sql.functions as func
from pyspark.sql.types import StructType, StringType, BooleanType, StructField


# region SECRETS
SF_URL = ""
SF_USER = ""
SF_PASSWORD = ""
SF_DATABASE = ""
SF_SCHEMA = ""
SF_WAREHOUSE = ""
SF_ROLE = ""
SF_SOURCE_NAME = ""

BLOB_ACCOUNT_KEY = ""
BLOB_ACCOUNT_NAME = ""
BLOB_CONTAINER_NAME = ""
# endregion


def is_mounted(path):
    if any(mount.mountPoint == path for mount in dbutils.fs.mounts()):
        return True
    return False


def mount_data():
    azure_key = BLOB_ACCOUNT_KEY  # clave del blob account
    azure_sa = BLOB_ACCOUNT_NAME  # nombre del blob account
    to_mount = BLOB_CONTAINER_NAME  # container name on blob storage
    mount_point = f"/mnt/{to_mount}/"

    if not is_mounted(mount_point):
        dbutils.fs.mount(
            source=f"wasbs://{to_mount}@{azure_sa}.blob.core.windows.net/",
            mount_point=mount_point,
            extra_configs={f"fs.azure.account.key.{azure_sa}.blob.core.windows.net": azure_key},
        )

    return mount_point


def get_snowflake_options():
    sf_options = {
        "sfUrl": SF_URL,
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "sfRole": SF_ROLE,
    }
    return sf_options


def process_row(row):
    dict_row = row.asDict()
    result = dict()
    result["pyar_more_two_minutes"] = dict_row["duration_ms"] > 60000
    result["pyar_generated_on"] = (
        str(datetime.strptime(dict_row["generated_on"], "%Y-%m-%d %H:%M:%S.%f").date())
        if dict_row["generated_on"] else None
    )
    return result


def main():
    # montamos la data y obtenemos la ruta donde estan los archivos
    mount_point = mount_data()

    # leemos data y la guardamos en el dataframe
    df = (
        spark.read.option("encoding", "utf-8").option("multiLine", True).json(f"{mount_point}")
        .select(['info.*', func.explode('playlists').alias('playlists')])
        .select(['generated_on', 'playlists.*'])
        .select(['generated_on', 'collaborative', 'description', 'modified_at', 'name', 'pid',
                 func.explode('tracks').alias('tracks')])
        .select(['generated_on', 'collaborative', 'description', 'modified_at', 'name', 'pid', 'tracks.*'])
    )
    # repartition the dataframe to be more efficient
    count = df.count()
    num_partitions = int(max(count / 100000, 200))
    df = df.repartition(num_partitions)
    df = df.persist()

    fields = [
        StructField("pyar_more_two_minutes", BooleanType()),
        StructField("pyar_generated_on", StringType()),
    ]

    my_udf = func.udf(lambda x: process_row(x), StructType(fields))
    new_df = df.withColumn("processed_data", my_udf(func.struct([df[col] for col in df.columns])))

    final_df = new_df.select([df[col] for col in df.columns] + ["processed_data.*"])

    final_df.write.format(SF_SOURCE_NAME).options(**get_snowflake_options()).option(
        "dbtable", "spotify_processed"
    ).mode("overwrite").save()

    df.unpersist()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
