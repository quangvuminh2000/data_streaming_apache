import sys
import logging
import json
import re
import argparse

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,  # Set to INFO or DEBUG for more detailed output
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class BCOLORS:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )

    print("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """
    )

    print("Table created successfully!")


def format_string_cassandra(text: str):
    return re.sub(r"'", r"''", text)


def insert_data(session, json_obj):
    # print("inserting data...")

    user_id = format_string_cassandra(json_obj.get("id"))
    first_name = format_string_cassandra(json_obj.get("first_name"))
    last_name = format_string_cassandra(json_obj.get("last_name"))
    gender = format_string_cassandra(json_obj.get("gender"))
    address = format_string_cassandra(json_obj.get("address"))
    postcode = json_obj.get("post_code")
    email = format_string_cassandra(json_obj.get("email"))
    username = format_string_cassandra(json_obj.get("username"))
    dob = format_string_cassandra(json_obj.get("dob"))
    registered_date = format_string_cassandra(json_obj.get("registered_date"))
    phone = format_string_cassandra(json_obj.get("phone"))
    picture = format_string_cassandra(json_obj.get("picture"))

    try:
        query = f"""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture)
                VALUES ('{user_id}', '{first_name}', '{last_name}', '{gender}', '{address}', '{postcode}', '{email}', '{username}', '{dob}', '{registered_date}', '{phone}', '{picture}')
        """
        session.execute(query)
        logging.info(f"Data inserted for {email} {first_name} {last_name}")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")
        print(f"Here is the query:\n{query}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config("spark.driver.bindAddress", "localhost")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(["localhost"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    return sel


if __name__ == "__main__":
    # create spark connection
    # spark_conn = create_spark_connection()

    # if spark_conn is not None:
    #     # connect to kafka with spark connection
    #     spark_df = connect_to_kafka(spark_conn)
    #     selection_df = create_selection_df_from_kafka(spark_df)
    #     print(f"{BCOLORS.OKBLUE}{selection_df}{BCOLORS.ENDC}")
    #     session = create_cassandra_connection()

    #     consumer = KafkaConsumer(
    #         'users_created',
    #         bootstrap_servers="localhost:9092",
    #         auto_offset_reset="earliest"
    #     )

    #     if session is not None:
    #         create_keyspace(session)
    #         create_table(session)
    #         # insert_data(session)

    #         logging.info("Streaming is being started...")

    #         streaming_query = (
    #             selection_df.writeStream.format("org.apache.spark.sql.cassandra")
    #             .option("checkpointLocation", "/tmp/checkpoint")
    #             .option("keyspace", "spark_streams")
    #             .option("table", "created_users")
    #             .start()
    #         )

    #         streaming_query.awaitTermination()
    #         session.shutdown()

    parser = argparse.ArgumentParser(description="Consumer stream to Cassandra DB.")
    parser.add_argument(
        "--mode",
        required=True,
        help="The mode of consuming the data",
        choices=["initial", "append"],
        default="append",
    )
    args = parser.parse_args()

    session = create_cassandra_connection()

    stream_mode = "latest"
    if args.mode == "initial":
        stream_mode = "earliest"

    consumer = KafkaConsumer(
        "users_created",
        bootstrap_servers="localhost:9092",
        auto_offset_reset=stream_mode,
    )

    if session is not None:
        create_keyspace(session)
        create_table(session)

        try:

            for message in consumer:
                json_msg = json.loads(message.value.decode("utf-8"))
                insert_data(session, json_msg)
        except KeyboardInterrupt as er:
            print("Consumer not running due to keyboard interruption.")
            sys.exit()
