from sqlalchemy import create_engine, text
import configparser
import os


def read_config(config_file):
    """
    Read the configuration file.

    :param config_file: Path to the configuration file
    :return: Dictionary with configuration parameters
    """
    
    config = configparser.ConfigParser()
    config.read(config_file)

    db_path = config.get('DATABASE', 'DB_PATH')
    db_name = config.get('DATABASE', 'DB_NAME')
    ddl_script = config.get('PATHS', 'DDL_SCRIPT')
    dml_script = config.get('PATHS', 'DML_SCRIPT')

    return {
        'db_path' : db_path,
        'db_name' : db_name,
        'ddl_script' : ddl_script,
        'dml_script' : dml_script
    }


def execute_sql_script(engine, script_path):
    """
    Execute a SQL script from a file using SQLAlchemy.

    :param engine: SQLAlchemy engine object
    :param script path: Path to SQL script file
    """

    with open(script_path, 'r') as file:
        sql_script = file.read()

    try:
        with engine.connect() as connection:
            for statement in sql_script.split(';'):
                if statement.strip():
                    connection.execute(text(statement))
    except Exception as e:
        print(f"An error occured while executing sript {script_path}: {e}")


def check_and_create_directory(directory_path):
    """
    Check if a directory exists and create it if it does not.
    
    :param directory_path: Path to the directory
    """
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
        except OSError as e:
            print(f"An error occurred while creating the directory: {e}")


def main():
    # Get the directory where the current Python script is located
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the parent directory
    project_directory = os.path.dirname(current_directory)

    # Construct the path to the parameter file
    parameter_file_path = os.path.join(project_directory, 'Setup', 'Parameters.ini')
    
    # Read the parameter file
    config = configparser.ConfigParser()
    config.read(parameter_file_path)

    # Check and create db_path if it does not exist
    check_and_create_directory(config.get('PATH','DB_PATH'))

    db_path_name = config.get('PATH','DB_PATH') + config.get('DATABASE', 'DB_NAME')

    # Creat the SQLAlchemy engine
    engine = create_engine(f'sqlite:///{db_path_name}')

    # Execute the DDL Script
    execute_sql_script(engine, config.get('PATH', 'DDL_SCRIPT'))

    # Execute the DML Script
    execute_sql_script(engine, config.get('PATH', 'DML_SCRIPT'))

if __name__ == "__main__":
    main()