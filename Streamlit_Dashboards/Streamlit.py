import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from Data_Information_tab import data_information
import os
import configparser

current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(current_directory)
parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

config = configparser.ConfigParser()
config.read(parameter_file_path)

db_path = f"{config.get('PATH', 'SQL_DB_PATH')}/{config.get('DATABASE', 'SQL_DB_NAME')}"

engine = create_engine(f"sqlite:///{db_path}")
Session = sessionmaker(bind=engine)
session = Session()

# Setup the app title
st.set_page_config(page_title="Call Center Analytics Dashboard", layout="wide")

# Main Header
st.title("Call Center Analytics Dashboard")

# Create Tabs
tabs = st.tabs(["Data Info", "Amazon", "Uber", "AT&T"])

with tabs[0]:
    st.header("Data Information")
    data_information(session=session, st=st, px=px, go=go)

with tabs[1]:
    st.header("Analytics for Amazon Data")

with tabs[2]:
    st.header("Analytics for Uber Data")

with tabs[3]:
    st.header("Analytics for AT&T Data")
