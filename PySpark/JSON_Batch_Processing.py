from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import (
    md5,
    concat_ws,
    col,
    concat,
    lit,
    when,
    to_timestamp,
    udf,
    coalesce,
    row_number,
    upper,
    floor,
)
from pyspark.sql.window import Window
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from sqlalchemy import create_engine, MetaData, update
from sqlalchemy.orm import sessionmaker
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from DB_Lookup import (
    connect_to_database,
    return_lookup_value,
    close_database_connection,
)
import os
import configparser
import sys


def database_df_maker(db_path, source_id, spark):
    engine = create_engine(f"sqlite:///{db_path}")
    query = f"SELECT CSD_ID AS HISTORIC_CSD_ID, SOURCE_SYSTEM_IDENTIFIER AS HISTORIC_SSI, SOURCE_HASH_KEY AS HISTORIC_HASHKEY FROM CSD_DATA_MART WHERE ACTIVE_FLAG = 1 AND SOURCE_ID = {source_id}"
    pandas_df = pd.read_sql(query, con=engine)
    if pandas_df.empty:
        # Define the schema for the empty DataFrame
        schema = StructType(
            [
                StructField("HISTORIC_CSD_ID", IntegerType(), True),
                StructField("HISTORIC_SSI", StringType(), True),
                StructField("HISTORIC_HASHKEY", StringType(), True),
            ]
        )
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    else:
        return spark.createDataFrame(pandas_df)


def xml_df_maker(xml_data, spark):
    # Define the schema for the DataFrame
    schema = StructType(
        [
            StructField("increment_id", StringType(), True),
            StructField("id", StringType(), True),
            StructField("data", StringType(), True),
        ]
    )

    # Parse the XML data
    parsed_data = []
    for record in xml_data:
        increment_id = record.get("increment_id")
        record_id = record.get("id")
        data = record.get("data")
        parsed_data.append((increment_id, record_id, data))

    # Create a DataFrame with the parsed data
    df = spark.createDataFrame(parsed_data, schema)

    # Define a UDF to parse the XML content and extract fields
    def parse_xml(xml_string):
        root = ET.fromstring(xml_string)
        return {
            "SUPPORT_IDENTIFIER": root.find("SUPPORT_IDENTIFIER").text,
            "CONTACT_REGARDING": root.find("CONTACT_REGARDING").text,
            "AGENT_CODE": root.find("AGENT_CODE").text,
            "DATE_OF_INTERACTION": root.find("DATE_OF_INTERACTION").text,
            "STATUS_OF_INTERACTION": root.find("STATUS_OF_INTERACTION").text,
            "TYPE_OF_INTERACTION": root.find("TYPE_OF_INTERACTION").text,
            "CUSTOMER_TYPE": root.find("CUSTOMER_TYPE").text,
            "CONTACT_DURATION": root.find("CONTACT_DURATION").text,
            "AFTER_CONTACT_WORK_TIME": root.find("AFTER_CONTACT_WORK_TIME").text,
            "INCIDENT_STATUS": root.find("INCIDENT_STATUS").text,
            "FIRST_CONTACT_SOLVE": root.find("FIRST_CONTACT_SOLVE").text,
            "TYPE_OF_RESOLUTION": root.find("TYPE_OF_RESOLUTION").text,
            "SUPPORT_RATING": root.find("SUPPORT_RATING").text,
            "TIME_STAMP": root.find("TIME_STAMP").text,
        }

    # Register the UDF
    parse_xml_udf = udf(
        parse_xml,
        StructType(
            [
                StructField("SUPPORT_IDENTIFIER", StringType(), True),
                StructField("CONTACT_REGARDING", StringType(), True),
                StructField("AGENT_CODE", StringType(), True),
                StructField("DATE_OF_INTERACTION", StringType(), True),
                StructField("STATUS_OF_INTERACTION", StringType(), True),
                StructField("TYPE_OF_INTERACTION", StringType(), True),
                StructField("CUSTOMER_TYPE", StringType(), True),
                StructField("CONTACT_DURATION", StringType(), True),
                StructField("AFTER_CONTACT_WORK_TIME", StringType(), True),
                StructField("INCIDENT_STATUS", StringType(), True),
                StructField("FIRST_CONTACT_SOLVE", StringType(), True),
                StructField("TYPE_OF_RESOLUTION", StringType(), True),
                StructField("SUPPORT_RATING", StringType(), True),
                StructField("TIME_STAMP", StringType(), True),
            ]
        ),
    )

    # Apply the UDF to extract the fields from the XML content
    df_parsed = df.withColumn("parsed_data", parse_xml_udf(df["data"]))

    # Select only the parsed data fields
    df_final = df_parsed.select("parsed_data.*")

    # Calculate HASHKEY with null values replaced by "NULL"
    df_final = df_final.withColumn(
        "HASHKEY",
        md5(
            concat_ws("||", *[coalesce(col(c), lit("NULL")) for c in df_final.columns])
        ),
    )

    # Show the final DataFrame
    df_final.show(truncate=False)

    return df_final


def get_agent_id(agent_name, db_path):
    if agent_name is None:
        return None
    agent_name = f"'{agent_name}'"
    engine, Session = connect_to_database(db_path)
    session = Session()
    agent_id = return_lookup_value(
        session, "CSD_AGENTS", "'UBER'", "AGENT_ID", agent_name, "PSEUDO_CODE"
    )
    close_database_connection(engine)
    return agent_id


def get_support_area_id(support_area, db_path):
    if support_area is None:
        return None
    support_area = f"'{support_area}'"
    engine, Session = connect_to_database(db_path)
    session = Session()
    support_area_id = return_lookup_value(
        session,
        "CSD_SUPPORT_AREAS",
        "'UBER'",
        "SUPPORT_AREA_ID",
        support_area,
        "SUPPORT_AREA_NAME",
    )
    close_database_connection(engine)
    return support_area_id


def get_customer_type_id(customer_type, db_path):
    if customer_type is None:
        return None
    customer_type = f"'{customer_type}'"
    engine, Session = connect_to_database(db_path)
    session = Session()
    customer_type_id = return_lookup_value(
        session,
        "CSD_CUSTOMER_TYPES",
        "'UBER'",
        "CUSTOMER_TYPE_ID",
        customer_type,
        "CUSTOMER_TYPE_NAME",
    )
    close_database_connection(engine)
    return customer_type_id


def data_transformer(database_df, xml_df, db_path, source_id, data_load_id):
    # Register Udfs
    get_agent_id_udf = udf(
        lambda agent_name: get_agent_id(agent_name, db_path), StringType()
    )
    get_customer_type_id_udf = udf(
        lambda customer_type: get_customer_type_id(customer_type, db_path), StringType()
    )
    get_support_area_id_udf = udf(
        lambda support_area: get_support_area_id(support_area, db_path), StringType()
    )

    # Join the XML data with the existing database data
    df = xml_df.join(
        database_df,
        xml_df["SUPPORT_IDENTIFIER"] == database_df["HISTORIC_SSI"],
        "left",
    )

    # Determine the router group
    router_df = df.withColumn(
        "ROUTER_GROUP",
        when(col("HISTORIC_HASHKEY").isNull(), "INSERT")
        .when(col("HASHKEY") == col("HISTORIC_HASHKEY"), "DUPLICATE")
        .otherwise("UPDATE"),
    )

    filter_df = router_df.filter(col("ROUTER_GROUP") != "DUPLICATE")

    # Correct timestamp parsing
    transformed_df = (
        filter_df.withColumn("SOURCE_ID", lit(source_id))
        .withColumn("SOURCE_SYSTEM_IDENTIFIER", col("SUPPORT_IDENTIFIER"))
        .withColumn("AGENT_ID", get_agent_id_udf(col("AGENT_CODE")))
        .withColumn(
            "INTERACTION_DATE",
            to_timestamp(col("DATE_OF_INTERACTION"), "yyyyMMddHHmmss"),
        )
        .withColumn(
            "SUPPORT_AREA_ID", get_support_area_id_udf(col("CONTACT_REGARDING"))
        )
        .withColumn("INTERACTION_STATUS", col("STATUS_OF_INTERACTION"))
        .withColumn("INTERACTION_TYPE", col("TYPE_OF_INTERACTION"))
        .withColumn("CUSTOMER_TYPE_ID", get_customer_type_id_udf(col("CUSTOMER_TYPE")))
        .withColumn("HANDLE_TIME", col("CONTACT_DURATION"))
        .withColumn("WORK_TIME", col("AFTER_CONTACT_WORK_TIME"))
        .withColumn(
            "FIRST_CONTACT_RESOLUTION",
            when(upper(col("FIRST_CONTACT_SOLVE")) == lit("TRUE"), 1)
            .when(upper(col("FIRST_CONTACT_SOLVE")) == lit("FALSE"), 0)
            .otherwise(None),
        )
        .withColumn("QUERY_STATUS", col("INCIDENT_STATUS"))
        .withColumn("SOLUTION_TYPE", col("TYPE_OF_RESOLUTION"))
        .withColumn("CUSTOMER_RATING", col("SUPPORT_RATING"))
        .withColumn("SOURCE_HASH_KEY", col("HASHKEY"))
        .withColumn("ROUTER_GROUP", col("ROUTER_GROUP"))
        .withColumn("HISTORIC_CSD_ID", col("HISTORIC_CSD_ID"))
        .withColumn("DATA_LOAD_ID", lit(data_load_id))
        .withColumn("START_DATE", lit(datetime.now()))
        .withColumn("END_DATE", lit(datetime.strptime("2099-12-31", "%Y-%m-%d")))
    )

    # Add IS_VALID_DATA column
    valid_record_check_df = transformed_df.withColumn(
        "IS_VALID_DATA",
        when(
            col("AGENT_ID").isNull()
            | col("INTERACTION_DATE").isNull()
            | col("SUPPORT_AREA_ID").isNull()
            | col("INTERACTION_STATUS").isNull()
            | col("INTERACTION_TYPE").isNull()
            | col("CUSTOMER_TYPE_ID").isNull()
            | col("HANDLE_TIME").isNull()
            | col("WORK_TIME").isNull()
            | col("FIRST_CONTACT_RESOLUTION").isNull()
            | col("QUERY_STATUS").isNull()
            | col("SOLUTION_TYPE").isNull()
            | col("CUSTOMER_RATING").isNull(),
            0,
        ).otherwise(1),
    )

    # Select and reorder the columns
    final_output_df = valid_record_check_df.select(
        "SOURCE_ID",
        "SOURCE_SYSTEM_IDENTIFIER",
        "AGENT_ID",
        "INTERACTION_DATE",
        "SUPPORT_AREA_ID",
        "INTERACTION_STATUS",
        "INTERACTION_TYPE",
        "CUSTOMER_TYPE_ID",
        "HANDLE_TIME",
        "WORK_TIME",
        "FIRST_CONTACT_RESOLUTION",
        "QUERY_STATUS",
        "SOLUTION_TYPE",
        "CUSTOMER_RATING",
        "SOURCE_HASH_KEY",
        "IS_VALID_DATA",
        "HISTORIC_CSD_ID",
        "ROUTER_GROUP",
        "DATA_LOAD_ID",
        "START_DATE",
        "END_DATE",
    )

    return final_output_df


def upsert_table(dataframe, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables["CSD_DATA_MART"]

    # Convert the DataFrame to Pandas DataFrame for easier manipulation with SQLAlchemy
    pandas_df = dataframe.toPandas()

    total_upsert_count = 0
    valid_count = 0
    invalid_count = 0

    for index, row in pandas_df.iterrows():
        row_dict = row.to_dict()

        # Convert datetime to string if not NaT, otherwise set to None
        row_dict["INTERACTION_DATE"] = (
            row_dict["INTERACTION_DATE"].strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row_dict["INTERACTION_DATE"])
            else None
        )
        row_dict["START_DATE"] = (
            row_dict["START_DATE"].strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row_dict["START_DATE"])
            else None
        )
        row_dict["END_DATE"] = (
            row_dict["END_DATE"].strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row_dict["END_DATE"])
            else None
        )

        # Map DataFrame columns to table columns
        mapped_row = {
            "SOURCE_SYSTEM_IDENTIFIER": row_dict.get("SOURCE_SYSTEM_IDENTIFIER"),
            "SOURCE_HASH_KEY": row_dict.get("SOURCE_HASH_KEY"),
            "SOURCE_ID": row_dict.get("SOURCE_ID"),
            "AGENT_ID": row_dict.get("AGENT_ID"),
            "INTERACTION_DATE": row_dict.get("INTERACTION_DATE"),
            "SUPPORT_AREA_ID": row_dict.get("SUPPORT_AREA_ID"),
            "INTERACTION_STATUS": row_dict.get("INTERACTION_STATUS"),
            "INTERACTION_TYPE": row_dict.get("INTERACTION_TYPE"),
            "CUSTOMER_TYPE_ID": row_dict.get("CUSTOMER_TYPE_ID"),
            "HANDLE_TIME": row_dict.get("HANDLE_TIME"),
            "WORK_TIME": row_dict.get("WORK_TIME"),
            "FIRST_CONTACT_RESOLUTION": row_dict.get("FIRST_CONTACT_RESOLUTION"),
            "QUERY_STATUS": row_dict.get("QUERY_STATUS"),
            "SOLUTION_TYPE": row_dict.get("SOLUTION_TYPE"),
            "CUSTOMER_RATING": row_dict.get("CUSTOMER_RATING"),
            "DATA_LOAD_ID": row_dict.get("DATA_LOAD_ID"),
            "IS_VALID_DATA": row_dict.get("IS_VALID_DATA"),
            "ACTIVE_FLAG": 1,
            "START_DATE": row_dict.get("START_DATE"),
            "END_DATE": row_dict.get("END_DATE"),
        }

        if row["ROUTER_GROUP"] == "INSERT":
            # Insert the new record
            insert_stmt = table.insert().values(**mapped_row)
            session.execute(insert_stmt)
            total_upsert_count += 1
        elif row["ROUTER_GROUP"] == "UPDATE":
            # Deactivate the old record (only if it is active)
            deactivate_stmt = (
                update(table)
                .where(
                    table.c.CSD_ID == row["HISTORIC_CSD_ID"], table.c.ACTIVE_FLAG == 1
                )
                .values(ACTIVE_FLAG=0, END_DATE=row_dict.get("START_DATE"))
            )
            session.execute(deactivate_stmt)
            # Insert the new record
            insert_stmt = table.insert().values(**mapped_row)
            session.execute(insert_stmt)
            total_upsert_count += 1

        if row_dict["IS_VALID_DATA"] == 1:
            valid_count += 1
        else:
            invalid_count += 1

    session.commit()
    session.close()

    return total_upsert_count, valid_count, invalid_count


def main(record_id, data_load_id):
    # Get the directory where the current Python script is located
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the parent directory
    project_directory = os.path.dirname(current_directory)

    # Construct the path to the parameter file
    parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

    # Read the parameter file
    config = configparser.ConfigParser()
    config.read(parameter_file_path)

    db_path = (
        f"{config.get('PATH', 'SQL_DB_PATH')}/{config.get('DATABASE', 'SQL_DB_NAME')}"
    )

    spark = SparkSession.builder.appName("XML Batch Processing").getOrCreate()

    engine, Session = connect_to_database(db_path)

    # Use the Session object correctly to create a session instance
    session = Session()
    source_id = return_lookup_value(
        session, "CSD_SOURCES", "'UBER'", "SOURCE_ID", "'UBER'", "SOURCE_NAME"
    )
    session.close()

    if source_id is None:
        print("Error: source_id is None. Exiting the script.")
        return

    close_database_connection(engine)

    historic_df = database_df_maker(db_path, source_id, spark)
    host_name = config.get("API", "HOSTNAME")
    port = config.get("API", "PORT")
    api_link = f"http://{host_name}:{port}/get_greater_than/{record_id}"
    response = requests.get(api_link)

    if response.status_code == 200:
        xml_data = response.json()
        new_df = xml_df_maker(xml_data, spark)
        transformed_df = data_transformer(
            historic_df, new_df, db_path, source_id, data_load_id
        )

        # Upsert the transformed data into the database table
        total_upsert_count, valid_count, invalid_count = upsert_table(
            transformed_df, db_path
        )

        # Calculate data valid percentage
        data_valid_percentage = (
            (valid_count / total_upsert_count) * 100 if total_upsert_count > 0 else 0
        )

        # Return the counts and percentage
        return {
            "TOTAL_UPSERT_COUNT": total_upsert_count,
            "VALID_COUNT": valid_count,
            "INVALID_COUNT": invalid_count,
            "DATA_VALID_PERCENTAGE": data_valid_percentage,
        }
    else:
        print(
            f"Failed to fetch data from API: {response.status_code} - {response.text}"
        )
        return None


if __name__ == "__main__":
    # For debugging purposes, you can provide the record id and data_load_id as arguments
    if len(sys.argv) != 3:
        print("Usage: python XML_Batch_Processing.py <record_id> <data_load_id>")
        sys.exit(1)

    record_id = sys.argv[1]
    data_load_id = int(sys.argv[2])

    result = main(record_id, data_load_id)
    print(result)
