import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { JourneyMapsComponent } from './journey-maps.component';

describe('JourneyMapsComponent', () => {
  let component: JourneyMapsComponent;
  let fixture: ComponentFixture<JourneyMapsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ JourneyMapsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(JourneyMapsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
