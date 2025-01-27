# Medical Data Space Project  

## Overview  

The **Medical Data Space Project** provides a comprehensive platform for managing, processing, and exploring healthcare data. It integrates **FHIR (Fast Healthcare Interoperability Resources)** standards with a robust backend and an interactive user interface (UI) for seamless data access and visualization. The project is ideal for medical data integration, exploration, and research.  

-> Read report (in french) for more details.

### Key Features  

- **ETL Pipeline**: Extract, transform, and load FHIR JSON data into a structured MySQL database.  
- **RESTful API**: Access patient records, encounters, conditions, and more via API endpoints.  
- **Interactive UI**: A user-friendly interface to explore and interact with healthcare data.  
- **Data Validation**: Ensures accuracy and completeness of processed data.  
- **Scalable Data Space**: Flexible architecture for large healthcare datasets.  

App Design :

![app Design](imgs/img0.png)

---

## Components  

### 1. ETL Pipeline  
The core ETL script (`healthcare_etl.py`) processes raw FHIR data by:  
- Extracting FHIR resources (e.g., patients, observations).  
- Transforming them into a clean, relational structure.  
- Loading them into MySQL tables (e.g., `patients`, `encounters`, `conditions`).  

### 2. REST API  
The API provides endpoints for querying patient data, encounters, observations, and more. It allows integration with external systems and tools.  

### 3. User Interface (UI)  
The project includes a web-based UI built using **Flask** and frontend technologies. The UI enables users to:  
- Search and browse patient records.  
- Visualize medical data such as observations, immunizations, and conditions.  
- Monitor the ETL process and validate data.  

---

## Installation  

### Requirements  

1. **Software**:  
   - Python 3.8+  
   - MySQL Server  
   - pip for Python dependencies  

2. **Libraries**:  
   Install dependencies using:  
   ```bash
   pip install -r requirements.txt  
   ```  

3. **FHIR Data**:  
   A folder of FHIR JSON files (e.g., `fhir/`) is required for the ETL process.  

---

### Setup  

1. **Clone the Repository**:
   
   ```bash
   git clone https://github.com/drisskhattabi6/Data-Space-for-Electronic-Medical-Records.git 
   cd Data-Space-for-Electronic-Medical-Records
   ```  

2. **Set Up MySQL**: Create **healthcare_db** Database :

   - Windows : Use **XAMPP Control Panel**, in MySQL Dashboard (PHPMyAdmin), and create **healthcare_db**.
   - Linux :
      - Start MySQL:  
        ```bash
        sudo systemctl start mysql  
        ```  
      - Create a database:  
        ```sql
        CREATE DATABASE healthcare_db;  
        ```  

3. **Prepare FHIR Data**:  
   Place your FHIR JSON files into a folder (e.g., `fhir/`).  

---

## Running the Project  

### 1. Run the ETL Pipeline  

Execute the ETL script to process data and load it into the database:  
```bash
python3 healthcare_etl.py --input_dir fhir --mysql_url "mysql+pymysql://username:password@localhost:3306/healthcare_db"
```  

### 2. Start the REST API  

Run the API server:  
```bash
python backend/api.py  
```  

### 3. Start the User Interface  

npm and angular must be install :
```bash
npm install -g @angular/cli
```  

Navigate to the `frontend` folder and start the UI:  
```bash
cd frontend  
ng serve
```  

The UI will be available at **`http://localhost:4200/`**.  

---

## UI Features  

1. **Upload Data**:
   user can upload data (FHIR Format)
   
   ![](imgs/img1.png)

3. **Patient Management**:  
   Search for patients by name or ID and view their medical history.

   ![](imgs/img3.png)
   ![](imgs/img2.png)

5. **Data Visualization**:  
   Visualize patient observations, conditions, immunizations, and encounters using charts and tables.  

  ![](imgs/img4.png)

---

## Troubleshooting  

- **Database Errors**:  
   Verify MySQL credentials and ensure the database is running.  

- **UI Issues**:  
   Ensure all dependencies are installed, and Flask is running.  

- **Large Datasets**:  
   Optimize MySQL configurations or use external storage for large data.  

This project creates a unified data space for healthcare professionals, researchers, and developers, enabling efficient data exploration, validation, and analysis.  

---

## Team :

- **Hamza Hafdaoui**
- **Chihab Eddine LIEFRID**
- **KHATTABI IDRISS**
- **Salma amgarou**
- **BOUFARHI AYMAN**

Module : DATA SPACES & DATA INTEGRATION & SEMANTIC INTEROPERABILITY

Supervisor : **Mrs. Ouafae Baida**
