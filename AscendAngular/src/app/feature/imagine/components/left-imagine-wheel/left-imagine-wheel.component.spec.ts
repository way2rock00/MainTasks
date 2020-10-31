import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LeftImagineWheelComponent } from './left-imagine-wheel.component';

describe('LeftImagineWheelComponent', () => {
  let component: LeftImagineWheelComponent;
  let fixture: ComponentFixture<LeftImagineWheelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LeftImagineWheelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LeftImagineWheelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
