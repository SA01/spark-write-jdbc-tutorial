import time

from pyspark.sql import DataFrame

from src.common import read_data_from_db, create_spark_session


def write_data(data_df: DataFrame, table: str, connection_str: str, username: str, password: str):
    properties = {"user":username, "password":password, "driver": "org.postgresql.Driver"}
    data_df.write.jdbc(url=connection_str, table=table, properties=properties)


def write_data_with_mode(
        data_df: DataFrame,
        table: str,
        connection_str: str,
        username: str,
        password: str,
        num_partitions: int,
        mode: str = "error"
    ):
    properties = {
        "user":username, "password":password, "driver": "org.postgresql.Driver", "numPartitions": str(num_partitions)
    }
    data_df.write.jdbc(url=connection_str, table=table, properties=properties, mode=mode)


def write_data_partitioned(
        data_df: DataFrame, table: str, connection_str: str, username: str, password: str, num_partitions: int
):
    data_df\
        .write\
        .format("jdbc")\
        .option("url", connection_str)\
        .option("dbtable", table)\
        .option("user", username)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .option("numPartitions", num_partitions)\
        .save()


if __name__ == '__main__':
    spark = create_spark_session()

    num_partitions = 16

    src_connection_str = "jdbc:postgresql://localhost:5432/Adventureworks"
    src_username = "postgres"
    src_password = "postgres"

    sales_df = read_data_from_db(
        spark=spark,
        connection_str=src_connection_str,
        username=src_username,
        password=src_password,
        table="sales.salesorderdetail"
    )\
    .repartition(num_partitions).checkpoint(eager=True)

    target_connection_str = "jdbc:postgresql://localhost:5433/postgres"
    target_username = "postgres"
    target_password = "target"

    write_data_with_mode(
        data_df=sales_df,
        table="sales",
        connection_str=target_connection_str,
        username=target_username,
        password=target_password,
        num_partitions=num_partitions,
        mode="overwrite"
    )

    time.sleep(10000)
