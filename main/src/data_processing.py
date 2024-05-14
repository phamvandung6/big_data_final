import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import os

# Thiết lập biến môi trường để submit job PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 pyspark-shell"
)
# ------Cấu hình Kafka------
KAFKA_TOPIC_NAME = "shopping_topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Đường dẫn đến file data source
data_path = "/home/dautay/Documents/Projects/lean_DE/big_data_final/dataset/customer_shoping_data.csv"

# ------Cấu hình MySQL------

mysql_url = "jdbc:mysql://localhost:3306/sales_db"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "shopping_db"
mysql_table_name = "shopping_data"

mysql_driver_class = "com.mysql.cj.jdbc.Driver"
mysql_user_name = "root"
mysql_password = "admin"
mysql_jdbc_url = (
    "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name
)


# Hàm chuyển đổi dataframe và lưu vào MySQL


# current_df: DataFrame hiện tại chứa dữ liệu cần lưu vào MySQL.
# epoc_id: ID của epoch (khoảng thời gian) hiện tại.
def save_to_mysql(current_df, epoc_id):
    # Thông tin đăng nhập để kết nối đến cơ sở dữ liệu
    db_credentials = {
        "user": mysql_user_name,
        "password": mysql_password,
        "driver": mysql_driver_class,
    }

    # processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    # df: DataFrame mới được tạo từ current_df, thêm hai cột mới là batch_id và total, cùng với cột day_of_week.
    df = (
        current_df.withColumn("batch_id", lit(epoc_id))
        .withColumn("total", col("price") * col("quantity"))
        .withColumn("day_of_week", date_format(col("invoice_date"), "EEEE"))
    )

    # Lưu DataFrame df vào cơ sở dữ liệu MySQL đã cấu hình thông qua JDBC
    df.write.jdbc(
        url=mysql_jdbc_url,
        table=mysql_table_name,
        mode="append",
        properties=db_credentials,
    )


if __name__ == "__main__":
    print("Đang bắt đầu xử lí dữ liệu.....")

    print("Bắt đầu lúc: ")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = (
        SparkSession.builder.appName("Data Processing with Kafka and MySQL")
        # Thiết lập chế độ master của Spark, là chế độ chạy trên máy tính cục bộ
        .master("local[*]")
        # cấu hình cho Spark để đảm bảo rằng các thư viện và file jar cần
        # thiết đảm bảo rằng Spark có thể tương tác với MySQL và Kafka một
        # cách đúng đắn và hiệu quả trong quá trình chạy ứng dụng.
        .config(
            "spark.jars",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config(
            "spark.executor.extraClassPath",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config(
            "spark.executor.extraLibrary",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .config(
            "spark.driver.extraClassPath",
            "file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/jsr166e-1.1.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/mysql-connector-java-8.0.30.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/spark-sql-kafka-0-10_2.12-3.5.0.jar,file:///home/dautay/Documents/Projects/lean_DE/big_data_final/my_envir/kafka-clients-3.7.0.jar",
        )
        .getOrCreate()
    )

    # Các thông báo lỗi sẽ được ghi vào log
    spark.sparkContext.setLogLevel("ERROR")

    # đọc dữ liệu từ Kafka bằng cách sử dụng PySpark Structured Streaming.
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    print("In ra schema của dataframe:")
    df.printSchema()
    # Chuyển các giá trị trong cột value của df sang string
    df = df.selectExpr("CAST(value AS STRING)")

    # Định nghĩa schema cho dataframe
    df_schema = (
        StructType()
        .add("invoice_no", StringType())
        .add("customer_id", StringType())
        .add("gender", StringType())
        .add("age", IntegerType())
        .add("category", StringType())
        .add("quantity", IntegerType())
        .add("price", FloatType())
        .add("payment_method", StringType())
        .add("invoice_date", DateType())
        .add("shopping_mall", StringType())
    )

    # phân tích cú pháp dữ liệu JSON từ cột "value" của DataFrame df sử
    # dụng schema ở trên trước đó. Kết quả là một cột mới được đặt tên là
    # "shopping" chứa các cặp key-value đã được phân tích cú pháp từ JSON.
    df = df.select(from_json(col("value"), df_schema).alias("shopping"))

    # Chọn tất cả các cột từ cột "shopping"
    df = df.select("shopping.*")

    # Ghi dữ liệu vào mysql, chia dữ liệu streaming thành các batch nhỏ, mỗi 5 giây thì nó sẽ được xử lí
    df.writeStream.trigger(processingTime="5 seconds").outputMode(
        "update"
    ).foreachBatch(save_to_mysql).start().awaitTermination()

    print("Chương trình streaming kết thúc")
