import streamlit as st
import plotly.express as px
import sqlite3
import pandas as pd
import os
import configparser

def connect_to_db(db_path):
    conn = sqlite3.connect(db_path)
    return conn

def fetch_data(conn, query):
    return pd.read_sql_query(query, conn)

def main():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)
    parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

    config = configparser.ConfigParser()
    config.read(parameter_file_path)

    db_path = (
        f"{config.get('PATH', 'SQL_DB_PATH')}/{config.get('DATABASE', 'SQL_DB_NAME')}"
    )
    st.title("Data Analysis Dashboard")

    # Connect to the database
    if db_path:
        conn = connect_to_db(db_path)
        table = 'CSD_DATA_MART'