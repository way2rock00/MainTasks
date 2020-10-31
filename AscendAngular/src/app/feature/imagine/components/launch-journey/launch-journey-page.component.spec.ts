import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LaunchJourneyPageComponent } from './launch-journey-page.component';

describe('LaunchJourneyPageComponent', () => {
  let component: LaunchJourneyPageComponent;
  let fixture: ComponentFixture<LaunchJourneyPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LaunchJourneyPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LaunchJourneyPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
