import { Component, OnInit } from '@angular/core';
import { MedicalRecordService } from '../medical-record.service';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { Observable, startWith, debounceTime, distinctUntilChanged, map } from 'rxjs';
import { query } from '@angular/animations';
import { CommonModule } from '@angular/common';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatIconModule } from '@angular/material/icon';
import {MatTableModule} from '@angular/material/table'; 
import { MatButtonModule } from '@angular/material/button';

@Component({
  selector: 'app-patients',
  standalone: true,
  imports: [CommonModule,MatButtonModule,ReactiveFormsModule,MatFormFieldModule,MatIconModule,MatTableModule],
  templateUrl: './patients.component.html',
  styleUrl: './patients.component.css'
})
export class PatientsComponent implements OnInit {
  searchControl = new FormControl(''); // Reactive form control for the search input
  patients: any[] = []; // Complete list of patients
  filteredPatients$!: Observable<any[]>; // Observable for filtered patients
  displayedColumns: string[] = [
    'patient_id',
    'full_name',
    'birth_date',
    'gender',
    'deceased_datetime',
    'encounter_count',
    'condition_count',
    'observation_count',
    'request_count',
    'procedure_count',
    'immunization_count',
    'careplan_count'
  ]; // Table columns

  constructor(private medicalRecordService: MedicalRecordService) {}

  ngOnInit() {
    // Fetch all patients and initialize filtering
    this.medicalRecordService.getAllPatients().subscribe((res) => {
      this.patients = res;

      // Initialize filtering once patients are fetched
      this.filteredPatients$ = this.searchControl.valueChanges.pipe(
        startWith(''), // Start with an empty string
        debounceTime(300), // Add a delay to reduce API calls
        distinctUntilChanged(), // Only process unique values
        map((query) => query ?? ''), // Default to an empty string if null
        map((query) =>
          this.patients.filter((patient) =>
            patient.full_name.toLowerCase().includes(query.toLowerCase())
          )
        )
      );
    });
  }
}

