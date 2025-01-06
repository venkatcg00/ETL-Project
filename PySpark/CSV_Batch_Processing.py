from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat_ws, col, concat, lit, when, to_date, date_format
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from DB_Lookup import connect_to_database, return_lookup_value, close_database_connection
import os
import configparser

# Get the directory where the current Python script is located
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate to the parent directory
project_directory = os.path.dirname(current_directory)

# Construct the path to the parameter file
parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

# Read the parameter file
config = configparser.ConfigParser()
config.read(parameter_file_path)

db_path = f"sqlite:///{config.get('PATH', 'SQL_DB_PATH')}/{config.get('DATABASE', 'SQL_DB_NAME')}"

def database_df_maker(db_path, source_id, spark):
    engine = create_engine(db_path)
    query = f'SELECT CSD_ID, SOURCE_SYSTEM_IDENTIFIER, SOURCE_HASH_KEY FROM CSD_DATA_MART WHERE ACTIVE_FLAG = 1 AND SOURCE_ID = {source_id}'
    pandas_df = pd.read_sql(query, con=engine)
    return spark.createDataFrame(pandas_df)

def csv_df_maker(file_path: str, spark):
    csv_schema = StructType([
        StructField("TICKET_IDENTIFIER", IntegerType(), True),
        StructField("SUPPORT_CATEGORY", StringType(), True),
        StructField("AGENT_NAME", StringType(), True),
        StructField("DATE_OF_CALL", StringType(), True),
        StructField("CALL_STATUS", StringType(), True),
        StructField("CALL_TYPE", StringType(), True),
        StructField("TYPE_OF_CUSTOMER", StringType(), True),
        StructField("DURATION", IntegerType(), True),
        StructField("WORK_TIME", IntegerType(), True),
        StructField("TICKET_STATUS", StringType(), True),
        StructField("RESOLVED_IN_FIRST_CONTACT", IntegerType(), True),
        StructField("RESOLUTION_CATEGORY", StringType(), True),
        StructField("RATING", StringType(), True)
    ])

    df = spark.read.csv(file_path, header=True, schema=csv_schema)
    df = df.withColumn("HASHKEY", md5(concat_ws('||', *[col(c) for c in df.columns])))
    df = df.withColumn("TICKET_IDENTIFIER", concat(lit("AT&T-"), col("TICKET_IDENTIFIER")))
    return df

def data_transformer(database_df, csv_df):
    df = csv_df.join(database_df, csv_df["TICKET_IDENTIFIER"] == database_df["SOURCE_SYSTEM_IDENTIFIER"], "left")
    router_df = df.withColumn("ROUTER_GROUP", 
                              when(col("SOURCE_HASH_KEY").isNull(), "INSERT")
                              .when(col("HASHKEY") == col("SOURCE_HASH_KEY"), "DUPLICATE")
                              .when(col("HASHKEY") != col("SOURCE_HASH_KEY"), "UPDATE"))

    filter_df = router_df.filter(col("ROUTER_GROUP") != "DUPLICATE")

    engine, sessionmaker = connect_to_database(
        config.get("PATH", "SQL_DB_PATH"), config.get("DATABASE", "SQL_DB_NAME")
    )

    source_id = return_lookup_value(sessionmaker, 'CSD_SOURCES', "'AT&T'", 'SOURCE_ID', "'AT&T'", 'SOURCE_NAME')

    transformed_df = filter_df.withColumn("SOURCE_ID", lit(source_id)) \
                              .withColumn("SOURCE_SYSTEM_IDENTIFIER", col("TICKET_IDENTIFIER")) \
                              .withColumn("AGENT_ID", return_lookup_value(sessionmaker, 'CSD_AGENTS', "'AT&T'", 'AGENT_ID', col("AGENT_NAME"), 'PSEUDO_CODE')) \
                              .withColumn("INTERACTION_DATE", to_date(date_format(col("DATE_OF_CALL"), "MMddyyyyHHmmss"), "MMddyyyyHHmmss")) \
                              .withColumn("SUPPORT_AREA_ID", return_lookup_value(sessionmaker, 'CSD_SUPPORT_AREAS', "'AT&T'", 'SUPPORT_AREA_ID', col("SUPPORT_CATEGORY"), 'SUPPORT_AREA_NAME')) \
                              .withColumn("INTERACTION_STATUS", col("CALL_STATUS")) \
                              .withColumn("INTERACTION_TYPE", col("CALL_TYPE")) \
                              .withColumn("CUSTOMER_TYPE_ID", return_lookup_value(sessionmaker, 'CSD_CUSTOMER_TYPES', "'AT&T'", 'CUSTOMER_TYPE_ID', col("TYPE_OF_CUSTOMER"), 'CUSTOMER_TYPE_NAME')) \
                              .withColumn("HANDLE_TIME", (col("DURATION") - col("WORK_TIME"))) \
                              .withColumn("WORK_TIME", col("WORK_TIME")) \
                              .withColumn("FIRST_CONTACT_RESOLUTION", col("RESOLVED_IN_FIRST_CONTACT")) \
                              .withColumn("QUERY_STATUS", col("TICKET_STATUS")) \
                              .withColumn("SOLUTION_TYPE", col("RESOLUTION_CATEGORY")) \
                              .withColumn("CUSTOMER_RATING", when(col("RATING") == "WORST", 1)
                                          .when(col("RATING") == "BAD", 2)
                                          .when(col("RATING") == "NEUTRAL", 3)
                                          .when(col("RATING") == "GOOD", 4)
                                          .when(col("RATING") == "BEST", 5)) \
                              .withColumn("SOURCE_HASH_KEY", col("HASHKEY"))

    close_database_connection(engine)

    valid_record_check_df = transformed_df.withColumn("IS_VALID_DATA", 
                                                      when(col("AGENT_ID").isNull() | 
                                                           col("INTERACTION_DATE").isNull() | 
                                                           col("SUPPORT_AREA_ID").isNull() | 
                                                           col("INTERACTION_STATUS").isNull() | 
                                                           col("INTERACTION_TYPE").isNull() | 
                                                           col("CUSTOMER_TYPE_ID").isNull() | 
                                                           col("HANDLE_TIME").isNull() | 
                                                           col("WORK_TIME").isNull() | 
                                                           col("FIRST_CONTACT_RESOLUTION").isNull() | 
                                                           col("QUERY_STATUS").isNull() | 
                                                           col("CUSTOMER_RATING").isNull(), 1).otherwise(0))

    return valid_record_check_df

def main(file_path):
    spark = SparkSession.builder.appName("CSV Batch Processing").getOrCreate()

    engine, sessionmaker = connect_to_database(
        config.get("PATH", "SQL_DB_PATH"), config.get("DATABASE", "SQL_DB_NAME")
    )

    source_id = return_lookup_value(sessionmaker, 'CSD_SOURCES', "'AT&T'", 'SOURCE_ID', "'AT&T'", 'SOURCE_NAME')

    close_database_connection(engine)

    historic_df = database_df_maker(db_path=db_path, source_id=source_id, spark=spark)
    new_df = csv_df_maker(file_path, spark)
    transformed_df = data_transformer(historic_df, new_df)

    transformed_df.show()

if __name__ == "__main__":
    file_path = "path/to/your/csvfile.csv"
    main(file_path)
