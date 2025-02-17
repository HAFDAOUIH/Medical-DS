from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from pathlib import Path
import os
import shutil
import pymysql
from healthcare_etl import HealthcareETL
from flask_cors import CORS, cross_origin
app = Flask(__name__)
cors = CORS(app)

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


def search_by_patient_id(cursor, patient_id):
    """Fetch all patient-related data by patient ID."""
    query = """
    SELECT 
        p.id AS patient_id,
        CONCAT(p.given_name, ' ', p.family_name) AS full_name,
        p.birth_date,
        p.gender,
        (SELECT COUNT(*) FROM encounter e WHERE e.patient_reference = p.id) AS encounter_count,
        (SELECT COUNT(*) FROM medical_condition mc WHERE mc.patient_reference = p.id) AS condition_count,
        (SELECT COUNT(*) FROM medical_observation mo WHERE mo.patient_reference = p.id) AS observation_count,
        (SELECT COUNT(*) FROM medicationrequest mr WHERE mr.patient_reference = p.id) AS request_count,
        (SELECT COUNT(*) FROM medical_procedure mp WHERE mp.patient_reference = p.id) AS procedure_count,
        (SELECT COUNT(*) FROM immunization im WHERE im.patient_reference = p.id) AS immunization_count,
        (SELECT COUNT(*) FROM careplan cp WHERE cp.patient_reference = p.id) AS careplan_count
    FROM 
        patient p
    WHERE 
        p.id = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchone()


def search_by_patient_name(cursor, name_pattern):
    """
    Fetch all patient-related data by a pattern in their name (family or given).

    name_pattern: Pattern to match in the patient's family or given name.
                      (e.g., '%John%' for partial matches)
    """
    query = """
    SELECT 
        p.id AS patient_id,
        CONCAT(p.given_name, ' ', p.family_name) AS full_name,
        p.birth_date,
        p.gender,
        p.deceased_datetime,
        (SELECT COUNT(*) FROM encounter e WHERE e.patient_reference = p.id) AS encounter_count,
        (SELECT COUNT(*) FROM medical_condition mc WHERE mc.patient_reference = p.id) AS condition_count,
        (SELECT COUNT(*) FROM medical_observation mo WHERE mo.patient_reference = p.id) AS observation_count,
        (SELECT COUNT(*) FROM medicationrequest mr WHERE mr.patient_reference = p.id) AS request_count,
        (SELECT COUNT(*) FROM medical_procedure mp WHERE mp.patient_reference = p.id) AS procedure_count,
        (SELECT COUNT(*) FROM immunization im WHERE im.patient_reference = p.id) AS immunization_count,
        (SELECT COUNT(*) FROM careplan cp WHERE cp.patient_reference = p.id) AS careplan_count
    FROM 
        patient p
    WHERE 
        p.family_name LIKE %s OR p.given_name LIKE %s;
    """
    # Use the same pattern for both given_name and family_name
    cursor.execute(query, (name_pattern, name_pattern))
    return cursor.fetchall()

def get_patients(cursor):
    query = """
    SELECT 
        p.id AS patient_id,
        CONCAT(p.given_name, ' ', p.family_name) AS full_name,
        p.birth_date,
        p.gender,
        p.deceased_datetime,
        (SELECT COUNT(*) FROM encounter e WHERE e.patient_reference = p.id) AS encounter_count,
        (SELECT COUNT(*) FROM medical_condition mc WHERE mc.patient_reference = p.id) AS condition_count,
        (SELECT COUNT(*) FROM medical_observation mo WHERE mo.patient_reference = p.id) AS observation_count,
        (SELECT COUNT(*) FROM medicationrequest mr WHERE mr.patient_reference = p.id) AS request_count,
        (SELECT COUNT(*) FROM medical_procedure mp WHERE mp.patient_reference = p.id) AS procedure_count,
        (SELECT COUNT(*) FROM immunization im WHERE im.patient_reference = p.id) AS immunization_count,
        (SELECT COUNT(*) FROM careplan cp WHERE cp.patient_reference = p.id) AS careplan_count
    FROM 
        patient p
    """
    # Use the same pattern for both given_name and family_name
    cursor.execute(query)
    return cursor.fetchall()

def get_dashboard_data(cursor):
    dashboard_data = {}

    # Number of Patients by Gender
    cursor.execute("""
        SELECT gender, COUNT(*) AS patient_count
        FROM patient
        GROUP BY gender;
    """)
    dashboard_data['patients_by_gender'] = cursor.fetchall()

    # Number of Conditions by Category
    cursor.execute("""
        SELECT code_text, COUNT(*) AS condition_count
        FROM medical_condition
        GROUP BY code_text;
    """)
    dashboard_data['conditions_by_category'] = cursor.fetchall()

    # Immunization Coverage Over Time
    cursor.execute("""
        SELECT 
            YEAR(occurrence_date) AS year,
            MONTH(occurrence_date) AS month,
            COUNT(DISTINCT patient_reference) AS immunization_count
        FROM immunization
        WHERE status = 'completed'
        GROUP BY YEAR(occurrence_date), MONTH(occurrence_date)
        ORDER BY year, month;
    """)
    dashboard_data['immunization_over_time'] = cursor.fetchall()

    # Number of Encounter Records Over Time
    cursor.execute("""
        SELECT 
            YEAR(start_date) AS year,
            MONTH(start_date) AS month,
            COUNT(*) AS encounter_count
        FROM encounter
        GROUP BY YEAR(start_date), MONTH(start_date)
        ORDER BY year, month;
    """)
    dashboard_data['encounters_over_time'] = cursor.fetchall()

    # Medication Administration Status Distribution
    cursor.execute("""
        SELECT status, COUNT(*) AS medication_count
        FROM medicationadministration
        GROUP BY status;
    """)
    dashboard_data['medication_status_distribution'] = cursor.fetchall()

    # Prevalence of Conditions by Code (Top 5)
    cursor.execute("""
        SELECT code_text, COUNT(DISTINCT patient_reference) AS condition_count
        FROM medical_condition
        GROUP BY code_text
        ORDER BY condition_count DESC
        LIMIT 5;
    """)
    dashboard_data['top_conditions'] = cursor.fetchall()

    return dashboard_data


@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    mysql_url = request.args.get('mysql_url')
    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        # Connect to the database
        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            # Retrieve dashboard data
            dashboard_data = get_dashboard_data(cursor)

        return jsonify(dashboard_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()

@app.route('/patients',methods=['GET'])
def getAllPatients():
    mysql_url = request.args.get('mysql_url')
    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400
    connection = None
    try:
        # Parse the database URL to extract connection details
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        # Connect to the database
        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Search by patient ID
            patient_data = get_patients(cursor)
            if not patient_data:
                return jsonify({"error": "Patients not found."}), 404
        
        return jsonify(patient_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


@app.route('/search_patient', methods=['GET'])
def search_patient_emr():
    """Endpoint for retrieving a patient's EMR from the database by ID or name."""
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')
    name = request.args.get('name')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id and not name:
        return jsonify({"error": "Either 'patient_id' or 'name' must be provided as a query parameter."}), 400

    connection = None
    try:
        # Parse the database URL to extract connection details
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        # Connect to the database
        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            if patient_id:
                # Search by patient ID
                patient_data = search_by_patient_id(cursor, patient_id)
                if not patient_data:
                    return jsonify({"error": "Patient not found."}), 404
            elif name:
                # Search by name (assume partial matching)
                name_pattern = f"%{name}%"  # Use wildcards for partial matching
                patient_data = search_by_patient_name(cursor, name_pattern)
                if not patient_data:
                    return jsonify({"error": "No patients found with the given name."}), 404

        return jsonify(patient_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


def get_all_encounters_by_patient_id(cursor, patient_id):

    query = """
    SELECT 
        e.id AS encounter_id,
        e.start_date,
        e.end_date,
        e.status
    FROM 
        encounter e
    WHERE 
        e.patient_reference = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchall()

def get_encounter_details_by_encounter_id(cursor, encounter_id):

    query = """
    SELECT 
        e.id AS encounter_id,
        e.start_date,
        e.end_date,
        e.status,
        (SELECT COUNT(*) FROM medical_observation mo WHERE mo.encounter_reference = e.id) AS observation_count,
        (SELECT COUNT(*) FROM medicationrequest mr WHERE mr.encounter_reference = e.id) AS request_count,
        (SELECT COUNT(*) FROM medical_procedure mp WHERE mp.encounter_reference = e.id) AS procedure_count
    FROM 
        encounter e
    WHERE 
        e.id = %s;
    """
    # (SELECT COUNT(*) FROM medical_condition mc WHERE mc.encounter_reference = e.id) AS condition_count,
    # (SELECT COUNT(*) FROM immunization im WHERE im.encounter_reference = e.id) AS immunization_count
    cursor.execute(query, (encounter_id,))
    return cursor.fetchone()


@app.route('/get_user_encounters', methods=['GET'])
def get_user_encounters():
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id:
        return jsonify({"error": "'patient_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        # Parse the database URL to extract connection details
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        # Connect to the database
        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Search by patient ID
            patient_data = get_all_encounters_by_patient_id(cursor, patient_id)
            if not patient_data:
                    return jsonify({"error": "Patient encounters not found."}), 404

        return jsonify(patient_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


@app.route('/get_encounter_details', methods=['GET'])
def get_encounter_details():
    mysql_url = request.args.get('mysql_url')
    encounter_id = request.args.get('encounter_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not encounter_id:
        return jsonify({"error": "'encounter_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Search by patient ID
            encounter_data = get_encounter_details_by_encounter_id(cursor, encounter_id)
            if not encounter_data:
                    return jsonify({"error": "Patient encounters not found."}), 404

        return jsonify(encounter_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


def get_observations_by_patient_id(cursor, patient_id):
    """ Fetch all observations for a given patient by their ID """

    query = """
        SELECT 
            mo.id AS observation_id,
            mo.encounter_reference,
            mo.effective_datetime,
            mo.issued,
            mo.value_quantity,
            mo.value_unit,
            mo.value_code,
            mo.status
        FROM 
            medical_observation mo
        WHERE 
            mo.patient_reference = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchall()


def get_immunizations_by_patient_id(cursor, patient_id):
    """
    Fetch all immunizations for a given patient by their ID.
    """
    query = """
    SELECT 
        id AS immunization_id,
        patient_reference,
        status,
        vaccine_code,
        occurrence_date,
        primary_source
    FROM 
        immunization
    WHERE 
        patient_reference = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchall()


def get_careplans_by_patient_id(cursor, patient_id):
    """
    Fetch all care plans for a given patient by their ID.
    """
    query = """
    SELECT 
        id AS careplan_id,
        patient_reference,
        status,
        intent,
        title,
        description,
        category_code,
        start_date,
        end_date
    FROM 
        careplan
    WHERE 
        patient_reference = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchall()


def get_medical_conditions_by_patient_id(cursor, patient_id):
    """
    Fetch all medical conditions for a given patient by their ID.
    """
    query = """
    SELECT 
        id AS condition_id,
        patient_reference,
        code_text,
        onset_datetime,
        abatement_datetime,
        recorded_date,
        verification_status
    FROM 
        medical_condition
    WHERE 
        patient_reference = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchall()


def get_medication_requests_by_patient_id(cursor, patient_id):
    """
    Fetch all medication requests for a given patient by their ID.
    """
    query = """
    SELECT 
        id AS request_id,
        patient_reference,
        encounter_reference,
        status,
        intent,
        medication_code,
        authored_on
    FROM 
        medicationrequest
    WHERE 
        patient_reference = %s;
    """
    cursor.execute(query, (patient_id,))
    return cursor.fetchall()


@app.route('/observations/patient', methods=['GET'])
def get_patient_observations():
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id:
        return jsonify({"error": "'patient_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            observations_data = get_observations_by_patient_id(cursor, patient_id)
            if not observations_data :
                    return jsonify({"error": "Patient observations not found."}), 404

        return jsonify(observations_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


@app.route('/immunizations/patient', methods=['GET'])
def get_patient_immunizations():
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id:
        return jsonify({"error": "'patient_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            immunizations_data = get_immunizations_by_patient_id(cursor, patient_id)
            if not immunizations_data :
                    return jsonify({"error": "Patient immunizations not found."}), 404

        return jsonify(immunizations_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


@app.route('/conditions/patient', methods=['GET'])
def get_patient_conditions():
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id:
        return jsonify({"error": "'patient_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            conditions_data = get_medical_conditions_by_patient_id(cursor, patient_id)
            if not conditions_data :
                    return jsonify({"error": "Patient conditions not found."}), 404

        return jsonify(conditions_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


@app.route('/medication-requests/patient', methods=['GET'])
def get_patient_medication_requests():
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id:
        return jsonify({"error": "'patient_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            medication_requests_data = get_medication_requests_by_patient_id(cursor, patient_id)
            if not medication_requests_data :
                    return jsonify({"error": "Patient medication requests not found."}), 404

        return jsonify(medication_requests_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


@app.route('/careplans/patient', methods=['GET'])
def get_patient_careplans():
    mysql_url = request.args.get('mysql_url')
    patient_id = request.args.get('patient_id')

    if not mysql_url:
        return jsonify({"error": "MySQL URL must be provided as a query parameter."}), 400

    if not patient_id:
        return jsonify({"error": "'patient_id' must be provided as a query parameter."}), 400

    connection = None
    try:
        url_parts = mysql_url.split('@')
        user_pass = url_parts[0].split('//')[1]
        user = user_pass.split(':')[0]
        db = url_parts[1].split('/')[-1]

        connection = pymysql.connect(
            host='127.0.0.1',
            user=user,
            password='',
            database=db,
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            careplans_data = get_careplans_by_patient_id(cursor, patient_id)
            if not careplans_data :
                    return jsonify({"error": "Patient careplans not found."}), 404

        return jsonify(careplans_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    app.run(debug=True)
