import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { InfoImagineWheelComponent } from './info-imagine-wheel.component';

describe('InfoImagineWheelComponent', () => {
  let component: InfoImagineWheelComponent;
  let fixture: ComponentFixture<InfoImagineWheelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ InfoImagineWheelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoImagineWheelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
