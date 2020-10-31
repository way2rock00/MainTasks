import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LaunchJourneyTabGroupComponent } from './launch-journey-tab-group.component';

describe('LaunchJourneyTabGroupComponent', () => {
  let component: LaunchJourneyTabGroupComponent;
  let fixture: ComponentFixture<LaunchJourneyTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LaunchJourneyTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LaunchJourneyTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
