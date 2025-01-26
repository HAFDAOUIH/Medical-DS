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
  dashboardData: any = null;
  errorMessage: string = '';

  constructor(private medicalRecordService: MedicalRecordService) { }

  ngOnInit(): void {
    this.fetchDashboardData();
  }

  fetchDashboardData(): void {
    this.medicalRecordService.getDashboardData().subscribe(
      (data) => {
        this.dashboardData = data;
      },
      (error) => {
        this.errorMessage = 'Error fetching dashboard data';
        console.error(error);
      }
    );
  }
  }