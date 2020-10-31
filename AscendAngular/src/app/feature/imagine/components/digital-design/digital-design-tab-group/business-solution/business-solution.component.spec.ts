import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BusinessSolutionComponent } from './business-solution.component';

describe('BusinessSolutionComponent', () => {
  let component: BusinessSolutionComponent;
  let fixture: ComponentFixture<BusinessSolutionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BusinessSolutionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BusinessSolutionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
