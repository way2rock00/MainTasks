import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeGeneratorStepperComponent } from './scope-generator-stepper.component';

describe('ScopeGeneratorStepperComponent', () => {
  let component: ScopeGeneratorStepperComponent;
  let fixture: ComponentFixture<ScopeGeneratorStepperComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeGeneratorStepperComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeGeneratorStepperComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
