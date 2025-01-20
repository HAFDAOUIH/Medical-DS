import { Component } from '@angular/core';
import { MedicalRecordService } from '../medical-record.service';
import {MatTabsModule} from '@angular/material/tabs';
import { CommonModule } from '@angular/common';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
@Component({
  selector: 'app-upload',
  standalone: true,
  imports: [CommonModule,MatTabsModule,CommonModule,MatIconModule,MatProgressSpinnerModule],
  templateUrl: './upload.component.html',
  styleUrl: './upload.component.css'
})
export class UploadComponent {
  selectedFiles: FileList | null = null;
  directoryFiles: File[] = [];
  isUploading = false;
  uploadMessage: string = '';

  constructor(private medicalRecordService: MedicalRecordService) {}

  // Method for single file selection
  onFileSelected(event: any): void {
    this.selectedFiles = event.target.files;
  }

  // Method for directory upload
  onDirectorySelected(event: any): void {
    this.directoryFiles = [];
    const files = event.target.files;
    for (let i = 0; i < files.length; i++) {
      this.directoryFiles.push(files[i]);
    }
  }

  // Method to upload files
  uploadFiles(): void {
    if (this.selectedFiles || this.directoryFiles.length > 0) {
      this.isUploading = true;
      const formData = new FormData();
      
      // Add files to formData
      if (this.selectedFiles) {
        Array.from(this.selectedFiles).forEach((file: File) => {
          formData.append('files', file, file.name);
        });
      }

      this.directoryFiles.forEach((file: File) => {
        formData.append('files', file, file.name);
      });

      this.medicalRecordService.uploadFiles(formData).subscribe({
        next: (response) => {
          this.uploadMessage = 'Files uploaded successfully!';
          this.isUploading = false;
        },
        error: (error) => {
          this.uploadMessage = 'Upload failed. Please try again.';
          this.isUploading = false;
        }
      });
    } else {
      this.uploadMessage = 'Please select files to upload.';
    }
  }
}
