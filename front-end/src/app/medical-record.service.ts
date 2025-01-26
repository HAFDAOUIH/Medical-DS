import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class MedicalRecordService {
  private baseUrl = 'http://127.0.0.1:5000'; // Base URL for the Flask API
  private mysqlUrl: string = 'mysql+pymysql://root@localhost:3306/healthcare_db'; // MySQL URL stored in the service

  constructor(private http: HttpClient) { }

  /**
   * Set the MySQL connection URL for future API requests.
   * @param url MySQL connection URL.
   */
  setMySqlUrl(url: string): void {
    this.mysqlUrl = url;
  }

  /**
   * Get the current MySQL connection URL.
   * @returns MySQL connection URL.
   */
  getMySqlUrl(): string {
    return this.mysqlUrl;
  }
  getUserEncounters(patientId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/get_user_encounters`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }

  getEncounterDetails(encounterId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/get_encounter_details`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('encounter_id', encounterId);

    return this.http.get(url, { params });
  }

  getPatientObservations(patientId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/observations/patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }

  getPatientImmunizations(patientId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/immunizations/patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }
  private validateMySqlUrl(): void {
    if (!this.mysqlUrl) {
      throw new Error('MySQL URL is not set');
    }
  }

  getPatientConditions(patientId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/conditions/patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }

  getPatientMedicationRequests(patientId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/medication-requests/patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }

  getPatientCareplans(patientId: string): Observable<any> {
    this.validateMySqlUrl();

    const url = `${this.baseUrl}/careplans/patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }
/**
   * Fetch dashboard data from the backend.
   * @returns Observable of the dashboard data.
   */
getDashboardData(): Observable<any> {
  if (!this.mysqlUrl) {
    throw new Error('MySQL URL is not set');
  }

  const url = `${this.baseUrl}/dashboard`;
  const params = new HttpParams().set('mysql_url', this.mysqlUrl);

  return this.http.get(url, { params });
}
  
  /**
   * Upload files or a folder for ETL processing.
   * @param formData FormData object containing files or folder.
   * @returns Observable with the API response.
   */
  uploadFiles(formData: FormData): Observable<any> {
    if (!this.mysqlUrl) {
      throw new Error('MySQL URL is not set');
    }
    formData.append('mysql_url', this.mysqlUrl); // Append MySQL URL to the form data

    const url = `${this.baseUrl}/upload`;
    return this.http.post(url, formData);
  }

  /**
   * Retrieve a patient's EMR data by ID.
   * @param patientId The ID of the patient.
   * @returns Observable with the patient's EMR data.
   */
  getPatientEMR(patientId: string): Observable<any> {
    if (!this.mysqlUrl) {
      throw new Error('MySQL URL is not set');
    }

    const url = `${this.baseUrl}/search_patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }

  getAllPatients():Observable<any>{
    if (!this.mysqlUrl) {
      throw new Error('MySQL URL is not set');
    }

    const url = `${this.baseUrl}/patients`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)

    return this.http.get(url, { params });
  }

  /**
   * Search for patients by name (partial match).
   * @param name The name (or part of it) to search for.
   * @returns Observable with the list of patients.
   */
  searchPatientsByName(name: string): Observable<any> {
    if (!this.mysqlUrl) {
      throw new Error('MySQL URL is not set');
    }

    const url = `${this.baseUrl}/search_patient`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('name', name);

    return this.http.get(url, { params });
  }

  /**
   * Get all encounters for a specific patient.
   * @param patientId The ID of the patient.
   * @returns Observable with the patient's encounters.
   */
  getPatientEncounters(patientId: string): Observable<any> {
    if (!this.mysqlUrl) {
      throw new Error('MySQL URL is not set');
    }

    const url = `${this.baseUrl}/get_user_encounters`;
    const params = new HttpParams()
      .set('mysql_url', this.mysqlUrl)
      .set('patient_id', patientId);

    return this.http.get(url, { params });
  }

  /**
   * Get the details of a specific encounter by encounter ID.
   * @param encounterId The ID of the encounter.
   * @returns Observable with the encounter details.
   */
  
}
