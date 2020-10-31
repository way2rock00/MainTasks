import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeDescriptionForm } from './scope-description.component';

describe('ScopeDescriptionForm', () => {
  let component: ScopeDescriptionForm;
  let fixture: ComponentFixture<ScopeDescriptionForm>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeDescriptionForm ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeDescriptionForm);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
