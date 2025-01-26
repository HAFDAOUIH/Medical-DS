import { Component, OnInit } from '@angular/core';
import { MedicalRecordService } from '../medical-record.service';
import Chart from 'chart.js/auto';
import { CommonModule } from '@angular/common';
@Component({
  selector: 'app-population-dashboard',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './population-dashboard.component.html',
  styleUrl: './population-dashboard.component.css'
})
export class PopulationDashboardComponent implements OnInit {
  dashboardData: any;

  constructor(private medicalRecordService: MedicalRecordService) { }

  ngOnInit(): void {
    this.loadDashboardData();
  }

  ngAfterViewInit(): void {
    // Ensure chart creation after the DOM is fully initialized
    if (this.dashboardData) {
      setTimeout(() => {
        this.createCharts();
      }, 0); // Use setTimeout to defer chart creation
    }
  }

  loadDashboardData(): void {
    this.medicalRecordService.getDashboardData().subscribe(
      (data) => {
        this.dashboardData = data;
        console.log(data); // Log the data to the console for debugging
        // Call chart creation here if data is already available
        if (this.dashboardData) {
          setTimeout(() => {
            this.createCharts();
          }, 0); // Defer chart creation until after data is loaded
        }
      },
      (error) => {
        console.error('Error fetching dashboard data:', error);
      }
    );
  }

  createCharts(): void {
    try {
      // 1. Gender Distribution Chart
      const genderChart = new Chart('genderChart', {
        type: 'pie',
        data: {
          labels: this.dashboardData.patients_by_gender.map((item: { gender: any; }) => item.gender),
          datasets: [{
            data: this.dashboardData.patients_by_gender.map((item: { patient_count: any; }) => item.patient_count),
            backgroundColor: ['#FF6384', '#36A2EB', '#FFCE56'],
          }]
        }
      });

      // 2. Conditions by Category Chart
      const conditionsCategoryChart = new Chart('conditionsCategoryChart', {
        type: 'bar',
        data: {
          labels: this.dashboardData.conditions_by_category.map((item: { code_text: any; }) => item.code_text),
          datasets: [{
            label: 'Conditions Count',
            data: this.dashboardData.conditions_by_category.map((item: { condition_count: any; }) => item.condition_count),
            backgroundColor: '#FF6384',
          }]
        }
      });

      // 3. Immunization Coverage Over Time Chart
      const immunizationChart = new Chart('immunizationChart', {
        type: 'line',
        data: {
          labels: this.dashboardData.immunization_over_time.map((item: { year: any; month: any; }) => `${item.year}-${item.month}`),
          datasets: [{
            label: 'Immunization Count',
            data: this.dashboardData.immunization_over_time.map((item: { immunization_count: any; }) => item.immunization_count),
            borderColor: '#36A2EB',
            fill: false,
          }]
        }
      });

      // 4. Encounter Records Over Time Chart
      const encounterChart = new Chart('encounterChart', {
        type: 'line',
        data: {
          labels: this.dashboardData.encounters_over_time.map((item: { year: any; month: any; }) => `${item.year}-${item.month}`),
          datasets: [{
            label: 'Encounter Count',
            data: this.dashboardData.encounters_over_time.map((item: { encounter_count: any; }) => item.encounter_count),
            borderColor: '#FFCE56',
            fill: false,
          }]
        }
      });

      // 5. Medication Administration Status Distribution
      const medicationStatusChart = new Chart('medicationStatusChart', {
        type: 'pie',
        data: {
          labels: this.dashboardData.medication_status_distribution.map((item: { status: any; }) => item.status),
          datasets: [{
            data: this.dashboardData.medication_status_distribution.map((item: { medication_count: any; }) => item.medication_count),
            backgroundColor: ['#FF6384', '#36A2EB', '#FFCE56'],
          }]
        }
      });

      // 6. Top Conditions Chart
      const topConditionsChart = new Chart('topConditionsChart', {
        type: 'bar',
        data: {
          labels: this.dashboardData.top_conditions.map((item: { code_text: any; }) => item.code_text),
          datasets: [{
            label: 'Condition Count',
            data: this.dashboardData.top_conditions.map((item: { condition_count: any; }) => item.condition_count),
            backgroundColor: '#FF6384',
          }]
        }
      });
    } catch (error) {
      console.error('Failed to create chart:', error);
    }
  }
  }