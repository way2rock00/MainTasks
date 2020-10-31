import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivityHeadComponent } from './activity-head.component';

describe('ActivityHeadComponent', () => {
  let component: ActivityHeadComponent;
  let fixture: ComponentFixture<ActivityHeadComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivityHeadComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivityHeadComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
