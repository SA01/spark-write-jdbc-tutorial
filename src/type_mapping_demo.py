from pyspark.pandas import DataFrame
from pyspark.sql.functions import expr, to_json, struct

from src.common import create_spark_session, read_data_from_db


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
        "user":username,
        "password":password,
        "driver": "org.postgresql.Driver",
        "numPartitions": str(num_partitions),
        "stringtype": "unspecified"
    }
    data_df.write.jdbc(url=connection_str, table=table, properties=properties, mode=mode)


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
        .withColumn("identifier", expr("uuid()"))\
        .withColumn("order_details", to_json(struct("orderqty", "productid", "unitprice")))\
        .repartition(num_partitions).checkpoint(eager=True)

    sales_df.show(truncate=False)
    sales_df.printSchema()

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
        # mode="overwrite"
        mode="append"
    )
