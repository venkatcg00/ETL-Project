import configparser
import os
import sys

# Get the absolute path of the current script
current_script_path = os.path.abspath(sys.argv[0])


# Make the Repository Path
repo_name = 'ETL-Project'
repo_path = current_script_path[:current_script_path.index(repo_name)] + 'ETL-Project/'


# Create a ConfigParser object
config = configparser.ConfigParser()


# Add a section and parameters to the configuration
config.add_section('PATH')
config.set('PATH', 'CSV_FOLDER', repo_path + 'Data/CSV_Files/')
config.set('PATH', 'DML_SCRIPT', repo_path + 'Setup/DML_Script.sql')
config.set('PATH', 'DDL_SCRIPT', repo_path + 'Setup/DDL_Script.sql')
config.set('PATH', 'DB_PATH', repo_path + 'Data/Untracked/')


# Add a new section for database details
config.add_section('DATABASE')
config.set('DATABASE', 'DB_NAME', 'CSD_DATABASE')


# Path to the new configuration file
config_file_path = repo_path + 'Setup/Parameters.ini'

# Write the configuration to a file
with open(config_file_path, 'w') as configfile:
    config.write(configfile)