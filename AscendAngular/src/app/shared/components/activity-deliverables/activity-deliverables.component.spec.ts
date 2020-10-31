import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivityDeliverablesComponent } from './activity-deliverables.component';

describe('ActivityDeliverablesComponent', () => {
  let component: ActivityDeliverablesComponent;
  let fixture: ComponentFixture<ActivityDeliverablesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivityDeliverablesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivityDeliverablesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
