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


def main():
    # Get the directory where the current Python script is located
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the parent directory
    project_directory = os.path.dirname(current_directory)

    # Construct the path to the parameter file
    parameter_file_path = os.path.join(project_directory, 'Setup', 'Parameters.ini')
    
    # Read the parameter file
    parameters = read_config(parameter_file_path)

    db_path = parameters['db_path']
    db_name = parameters['db_name']
    ddl_script = parameters['ddl_script']
    dml_script = parameters['dml_script']

    # Creat the SQLAlchemy engine
    engine = create_engine(f'sqlite:///{db_path}/{db_name}')

    # Execute the DDL Script
    execute_sql_script(engine, ddl_script)

    # Execute the DML Script
    execute_sql_script(engine, dml_script)

if __name__ == "__main__":
    main()