import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeWrapperStepperComponent } from './scope-wrapper-stepper.component';

describe('ScopeWrapperStepperComponent', () => {
  let component: ScopeWrapperStepperComponent;
  let fixture: ComponentFixture<ScopeWrapperStepperComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeWrapperStepperComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeWrapperStepperComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
