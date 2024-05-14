from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 pyspark-shell"
)

kafka_topic_name = "orderstopic"
kafka_bootstrap_servers = "localhost:9092"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user_name = "root"
mysql_password = "admin"
mysql_jdbc_url = (
    "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name
)

cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "sales_ks"
cassandra_table_name = "orders_tbl"

customers_data_file_path = (
    "/home/dautay/Documents/Projects/lean_DE/big_data_final/dataset/customers.csv"
)


url = "jdbc:mysql://localhost:3306/sales_db"

db_credentials = {
    "user": "root",
    "password": "admin",
    "driver": "com.mysql.jdbc.Driver",
}

table_name = "orders"


def save_to_cassandra(current_df, epoc_id):
    print("Printing epoc_id: ")
    print(epoc_id)

    print("Printing before Cassandra table save: " + str(epoc_id))
    current_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table=cassandra_table_name, keyspace=cassandra_keyspace_name
    ).save()
    print("Printing before Cassandra table save: " + str(epoc_id))


def save_to_mysql(current_df, epoc_id):
    db_credentials = {
        "user": mysql_user_name,
        "password": mysql_password,
        "driver": mysql_driver_class,
    }

    print("Printing epoc_id: ")
    print(epoc_id)

    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    current_df_final = current_df.withColumn(
        "processed_at", lit(processed_at)
    ).withColumn("batch_id", lit(epoc_id))

    print("Printing before MySQL table save: " + str(epoc_id))
    current_df_final.write.jdbc(
        url=mysql_jdbc_url,
        table=mysql_table_name,
        mode="append",
        properties=db_credentials,
    )
    print("Printing after MySQL table save: " + str(epoc_id))


if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = (
        SparkSession.builder.appName(
            "PySpark Structured Streaming with Kafka and Cassandra"
        )
        .master("local[*]")
        .config(
            "spark.jars",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-cassandra-connector_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config(
            "spark.executor.extraClassPath",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-cassandra-connector_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config(
            "spark.executor.extraLibrary",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-cassandra-connector_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config(
            "spark.driver.extraClassPath",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-cassandra-connector_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config("spark.cassandra.connection.host", cassandra_host_name)
        .config("spark.cassandra.connection.port", cassandra_port_no)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from orderstopic
    orders_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic_name)
        .option("startingOffsets", "latest")
        .load()
    )

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the orders data
    orders_schema = (
        StructType()
        .add("order_id", StringType())
        .add("created_at", StringType())
        .add("discount", StringType())
        .add("product_id", StringType())
        .add("quantity", StringType())
        .add("subtotal", StringType())
        .add("tax", StringType())
        .add("total", StringType())
        .add("customer_id", StringType())
    )

    orders_df2 = orders_df1.select(
        from_json(col("value"), orders_schema).alias("orders"), "timestamp"
    )

    orders_df3 = orders_df2.select("orders.*")

    orders_df3.writeStream.trigger(processingTime="15 seconds").outputMode(
        "update"
    ).foreachBatch(save_to_cassandra).start()

    customers_df = spark.read.csv(
        customers_data_file_path, header=True, inferSchema=True
    )
    customers_df.printSchema()
    customers_df.show(5, False)

    orders_df4 = orders_df3.join(
        customers_df, orders_df3.customer_id == customers_df.ID, how="inner"
    )
    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()

    # Simple aggregate - find total_sum_amount by grouping source, state
    orders_df5 = (
        orders_df4.groupBy("source", "state")
        .agg({"total": "sum"})
        .select("source", "state", col("sum(total)").alias("total_sum_amount"))
    )

    print("Printing Schema of orders_df5: ")
    orders_df5.printSchema()

    # Write final result into console for debugging purpose
    trans_detail_write_stream = (
        orders_df5.writeStream.trigger(processingTime="15 seconds")
        .outputMode("update")
        .option("truncate", "false")
        .format("console")
        .start()
    )

    orders_df5.writeStream.trigger(processingTime="15 seconds").outputMode(
        "update"
    ).foreachBatch(save_to_mysql).start()

    trans_detail_write_stream.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")
