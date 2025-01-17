import os
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from pathlib import Path
from healthcare_etl import HealthcareETL

app = Flask(__name__)

# Configuration
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['ALLOWED_EXTENSIONS'] = {'json', 'xml', 'csv'}

# Ensure the upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

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
    print('uploaded_files',  uploaded_files)
    print('folder_path : ', folder_path)

    saved_files = []

    # Process uploaded files
    if uploaded_files:
        for file in uploaded_files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(file_path)
                saved_files.append(file_path)
            else:
                return jsonify({"error": f"Unsupported file type for file: {file.filename}. Allowed types: {', '.join(app.config['ALLOWED_EXTENSIONS'])}"}), 400

    # Process folder path
    if folder_path:
        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            return jsonify({"error": f"Invalid folder path: {folder_path}"}), 400
        for file in folder.glob("**/*"):
            if file.is_file() and allowed_file(file.name):
                saved_files.append(str(file))

    if not saved_files:
        return jsonify({"error": "No valid files found to process."}), 400

    # Run ETL on all saved files
    try:
        etl = HealthcareETL(Path(app.config['UPLOAD_FOLDER']))
        mysql_url = os.getenv('MYSQL_URL', 'mysql+pymysql://user:pass@localhost/healthcare_db')
        etl.run_pipeline(mysql_url, files=saved_files)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"message": "Files processed successfully.", "processed_files": saved_files}), 200


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
