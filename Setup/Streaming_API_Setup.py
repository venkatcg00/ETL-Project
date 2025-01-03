import os
import xml.etree.ElementTree as ET
import configparser
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

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

# Get the directory where the current Python script is located
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate to the parent directory
project_directory = os.path.dirname(current_directory)

# Construct the path to the parameter file
parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

# Read the parameter file
config = configparser.ConfigParser()
config.read(parameter_file_path)

xml_path = config.get('PATH', 'XML_STORAGE')

# Check and create xml_path if it does not exist
check_and_create_directory(xml_path)

# Route to receive the XML data with record ID and store it as a file
@app.route('/store-xml', methods=['POST'])
def store_xml():
    try:
        # Get XML data and record ID from the request body
        data = request.get_json()
        xml_data = data.get('xml_data')
        record_id = data.get('record_id')

        # Ensure both xml_data and record_id are provided
        if not xml_data or not record_id:
            return jsonify({"error": "Both 'xml_data' and 'record_id' must be provided"}), 400

        # Parse the XML to ensure it is well-formed
        ET.fromstring(xml_data)

        # Define the file path using the provided record ID
        file_path = os.path.join(xml_path, f"{record_id}.xml")

        # Save the XML data to the file
        with open(file_path, 'w') as file:
            file.write(xml_data)

        # Return the record ID as confirmation
        return jsonify({"message": "XML data stored successfully", "record_id": record_id}), 201
    except ET.ParseError as e:
        return jsonify({"error": "Invalid XML data", "details": str(e)}), 400

# Route to retrieve stored XML data by record ID
@app.route('/get-xml/<record_id>', methods=['GET'])
def get_xml(record_id):
    # Define the file path using the record ID
    file_path = os.path.join(xml_path, f"{record_id}.xml")

    # Check if the file exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            xml_data = file.read()
        return Response(xml_data, mimetype='application/xml')
    else:
        return jsonify({"error": "Record ID not found"}), 404

# Route to clear all stored XML data
@app.route('/clear-xml', methods=['DELETE'])
def clear_xml():
    # Delete all files in the storage directory
    for file_name in os.listdir(xml_path):
        file_path = os.path.join(xml_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
    return jsonify({"message": "XML data cleared successfully"}), 200

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True)
