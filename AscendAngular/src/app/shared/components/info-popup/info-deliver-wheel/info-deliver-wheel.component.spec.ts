import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InfoDeliverWheelComponent } from './info-deliver-wheel.component';

describe('InfoDeliverWheelComponent', () => {
  let component: InfoDeliverWheelComponent;
  let fixture: ComponentFixture<InfoDeliverWheelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InfoDeliverWheelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoDeliverWheelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
