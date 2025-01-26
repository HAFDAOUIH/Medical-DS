import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PopulationDashboardComponent } from './population-dashboard.component';

describe('PopulationDashboardComponent', () => {
  let component: PopulationDashboardComponent;
  let fixture: ComponentFixture<PopulationDashboardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PopulationDashboardComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(PopulationDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
