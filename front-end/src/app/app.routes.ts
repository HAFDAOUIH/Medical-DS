import { Routes } from '@angular/router';
import { UploadComponent } from './upload/upload.component';
import { PatientsComponent } from './patients/patients.component';
import { EmrComponent } from './emr/emr.component';

export const routes: Routes = [
    { path: 'upload', component: UploadComponent },
    { path: 'patients', component: PatientsComponent},
    { path: 'patients/:id', component: EmrComponent }
];
