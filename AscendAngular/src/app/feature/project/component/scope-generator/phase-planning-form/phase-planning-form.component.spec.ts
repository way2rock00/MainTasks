import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PhasePlanningFormComponent } from './phase-planning-form.component';

describe('PhasePlanningFormComponent', () => {
  let component: PhasePlanningFormComponent;
  let fixture: ComponentFixture<PhasePlanningFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PhasePlanningFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PhasePlanningFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
