import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { MedicalRecordService } from '../medical-record.service';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import {MatExpansionModule} from '@angular/material/expansion';

@Component({
  selector: 'app-emr',
  standalone: true,
  imports: [CommonModule,MatTableModule,MatButtonModule,MatIconModule,MatExpansionModule],
  templateUrl: './emr.component.html',
  styleUrl: './emr.component.css'
})
export class EmrComponent {
  patient: any;
  encounters: any[] = [];
  encounterDetails: any;
  observations: any[] = [];
  immunizations: any[] = [];
  carePlans: any[] = [];
  conditions: any[] = [];
  medicationRequests: any[] = [];

  constructor(private route: ActivatedRoute, private medicalRecordService: MedicalRecordService) {}

  ngOnInit(): void {
    const patientId = this.route.snapshot.paramMap.get('id');
    if (patientId) {
      this.fetchPatientDetails(patientId);
      this.fetchPatientEncounters(patientId);
      this.fetchObservations(patientId);
      this.fetchImmunizations(patientId);
      this.fetchCarePlans(patientId);
      this.fetchConditions(patientId);
      this.fetchMedicationRequests(patientId);
    }
  }

  fetchPatientDetails(patientId: string): void {
    this.medicalRecordService.getPatientEMR(patientId).subscribe((data) => (this.patient = data));
  }

  fetchPatientEncounters(patientId: string): void {
    this.medicalRecordService.getPatientEncounters(patientId).subscribe((data) => (this.encounters = data));
  }

  fetchObservations(patientId: string): void {
    this.medicalRecordService.getPatientObservations(patientId).subscribe((data) => (this.observations = data));
  }

  fetchImmunizations(patientId: string): void {
    this.medicalRecordService.getPatientImmunizations(patientId).subscribe((data) => (this.immunizations = data));
  }

  fetchCarePlans(patientId: string): void {
    this.medicalRecordService.getPatientCareplans(patientId).subscribe((data) => (this.carePlans = data));
  }

  fetchConditions(patientId: string): void {
    this.medicalRecordService.getPatientConditions(patientId).subscribe((data) => (this.conditions = data));
  }

  fetchMedicationRequests(patientId: string): void {
    this.medicalRecordService.getPatientMedicationRequests(patientId).subscribe((data) => (this.medicationRequests = data));
  }

  viewEncounterDetails(encounterId: string): void {
    this.medicalRecordService.getEncounterDetails(encounterId).subscribe((data) => (this.encounterDetails = data));
  }
}