import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BusinessProcessComponent } from './business-process.component';

describe('BusinessProcessComponent', () => {
  let component: BusinessProcessComponent;
  let fixture: ComponentFixture<BusinessProcessComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BusinessProcessComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BusinessProcessComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
