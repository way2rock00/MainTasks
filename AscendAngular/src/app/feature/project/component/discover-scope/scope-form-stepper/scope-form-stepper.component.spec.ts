import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeFormStepperComponent } from './scope-form-stepper.component';

describe('ScopeFormStepperComponent', () => {
  let component: ScopeFormStepperComponent;
  let fixture: ComponentFixture<ScopeFormStepperComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeFormStepperComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeFormStepperComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
