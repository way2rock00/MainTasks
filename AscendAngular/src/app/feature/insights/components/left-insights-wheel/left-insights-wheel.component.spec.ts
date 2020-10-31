import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LeftInsightsWheelComponent } from './left-insights-wheel.component';

describe('LeftInsightsWheelComponent', () => {
  let component: LeftInsightsWheelComponent;
  let fixture: ComponentFixture<LeftInsightsWheelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LeftInsightsWheelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LeftInsightsWheelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
