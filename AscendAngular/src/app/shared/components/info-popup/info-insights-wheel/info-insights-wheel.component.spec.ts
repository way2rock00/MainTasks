import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InfoInsightsWheelComponent } from './info-insights-wheel.component';

describe('InfoInsightsWheelComponent', () => {
  let component: InfoInsightsWheelComponent;
  let fixture: ComponentFixture<InfoInsightsWheelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InfoInsightsWheelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoInsightsWheelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
