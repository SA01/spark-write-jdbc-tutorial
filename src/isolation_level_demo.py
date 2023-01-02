from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession

from src.common import read_data_from_db


def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "8g")

    spark_session = SparkSession\
        .builder\
        .master("local[2]")\
        .config(conf=conf)\
        .appName("Read from JDBC tutorial") \
        .config("spark.jars", "postgresql-42.5.1.jar") \
        .getOrCreate()

    spark_session.sparkContext.setCheckpointDir("checkpoint")

    return spark_session


def write_data_with_isolation_level(
        data_df: DataFrame,
        table: str,
        connection_str: str,
        username: str,
        password: str,
        num_partitions: int,
        mode: str = "error"
    ):
    properties = {
        "user":username,
        "password":password,
        "driver": "org.postgresql.Driver",
        "numPartitions": str(num_partitions),
        "isolationLevel": "SERIALIZABLE",
    }
    data_df.write.jdbc(url=connection_str, table=table, properties=properties, mode=mode)


if __name__ == '__main__':
    spark = create_spark_session()

    num_partitions = 128

    src_connection_str = "jdbc:postgresql://localhost:5432/Adventureworks"
    src_username = "postgres"
    src_password = "postgres"

    sales_df = read_data_from_db(
        spark=spark,
        connection_str=src_connection_str,
        username=src_username,
        password=src_password,
        table="sales.salesorderdetail"
    )

    for i in range(1,20):
        sales_df = sales_df.unionByName(
            read_data_from_db(
                spark=spark,
                connection_str=src_connection_str,
                username=src_username,
                password=src_password,
                table="sales.salesorderdetail"
            )
        )

    sales_df = sales_df.repartition(num_partitions).checkpoint(eager=True)

    print(f"Number of rows: {sales_df.count()}")

    target_connection_str = "jdbc:postgresql://localhost:5433/postgres"
    target_username = "postgres"
    target_password = "target"

    write_data_with_isolation_level(
        data_df=sales_df,
        table="sales",
        connection_str=target_connection_str,
        username=target_username,
        password=target_password,
        num_partitions=num_partitions,
        mode="append"
    )
