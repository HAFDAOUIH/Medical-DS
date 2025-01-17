from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from pathlib import Path
import os
import shutil
from healthcare_etl import HealthcareETL

app = Flask(__name__)

# Configuration
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['TMP_FOLDER'] = './tmp'
app.config['ALLOWED_EXTENSIONS'] = {'json', 'xml', 'csv'}

# Ensure the upload and tmp folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TMP_FOLDER'], exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


@app.route('/upload', methods=['POST'])
def upload_files():
    """Endpoint for uploading single or multiple files or a folder for ETL processing."""
    # Check if files or a folder are in the request
    if 'files' not in request.files and 'folder' not in request.form:
        return jsonify({"error": "No files or folder provided in the request."}), 400

    uploaded_files = request.files.getlist('files')
    folder_path = request.form.get('folder')
    mysql_url = request.form.get('mysql_url')

    if not mysql_url:
        return jsonify({"error": "MySQL URL not provided."}), 400

    tmp_folder = Path(app.config['TMP_FOLDER']) / 'etl_tmp'
    if tmp_folder.exists():
        shutil.rmtree(tmp_folder)
    os.makedirs(tmp_folder, exist_ok=True)

    # Process uploaded files
    if uploaded_files:
        for file in uploaded_files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = tmp_folder / filename
                file.save(file_path)
            else:
                return jsonify({"error": f"Unsupported file type for file: {file.filename}. Allowed types: {', '.join(app.config['ALLOWED_EXTENSIONS'])}"}), 400

    # Process folder path
    if folder_path:
        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            return jsonify({"error": f"Invalid folder path: {folder_path}"}), 400
        for file in folder.glob("**/*"):
            if file.is_file() and allowed_file(file.name):
                shutil.copy(file, tmp_folder / file.name)

    # Ensure there are files to process
    if not any(tmp_folder.iterdir()):
        return jsonify({"error": "No valid files found to process."}), 400

    # Run ETL on the tmp folder
    try:
        etl = HealthcareETL(tmp_folder)
        etl.run_pipeline(mysql_url)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        # Clean up tmp folder
        shutil.rmtree(tmp_folder, ignore_errors=True)

    return jsonify({"message": "Files processed successfully."}), 200


@app.route('/emr/<string:patient_id>', methods=['GET'])
def get_patient_emr(patient_id):
    """Endpoint for retrieving a patient's EMR from the database."""
    try:
        # Connect to the database and retrieve patient data
        mysql_url = os.getenv('MYSQL_URL', 'mysql+pymysql://user:pass@localhost/healthcare_db')
        etl = HealthcareETL()
        patient_data = etl.get_patient_emr(mysql_url, patient_id)

        if not patient_data:
            return jsonify({"error": "Patient not found."}), 404

        return jsonify(patient_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
