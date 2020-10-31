import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InsightsActivitiesPageComponent } from './insights-activities-page.component';

describe('InsightsActivitiesPageComponent', () => {
  let component: InsightsActivitiesPageComponent;
  let fixture: ComponentFixture<InsightsActivitiesPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InsightsActivitiesPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InsightsActivitiesPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
