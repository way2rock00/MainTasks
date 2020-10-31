import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InfoRunWheelComponent } from './info-run-wheel.component';

describe('InfoRunWheelComponent', () => {
  let component: InfoRunWheelComponent;
  let fixture: ComponentFixture<InfoRunWheelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InfoRunWheelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoRunWheelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
