import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivitiesFilterComponent } from './activities-filter.component';

describe('ActivitiesFilterComponent', () => {
  let component: ActivitiesFilterComponent;
  let fixture: ComponentFixture<ActivitiesFilterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivitiesFilterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivitiesFilterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
