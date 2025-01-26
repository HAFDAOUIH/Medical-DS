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

  constructor(
    private route: ActivatedRoute,
    private medicalRecordService: MedicalRecordService
  ) {}

  ngOnInit(): void {
    // Get patient ID from route
    const patientId = this.route.snapshot.paramMap.get('id');

    if (patientId) {
      this.fetchPatientDetails(patientId);
      this.fetchPatientEncounters(patientId);
    }
  }

  fetchPatientDetails(patientId: string): void {
    this.medicalRecordService.getPatientEMR(patientId).subscribe((data) => {
      this.patient = data;
    });
  }

  fetchPatientEncounters(patientId: string): void {
    this.medicalRecordService.getPatientEncounters(patientId).subscribe((data) => {
      this.encounters = data;
    });
  }

  viewEncounterDetails(encounterId: string): void {
    this.medicalRecordService.getEncounterDetails(encounterId).subscribe((data) => {
      this.encounterDetails = data;
    });
  }
}
