import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivityTabsComponent } from './activity-tabs.component';

describe('ActivityTabsComponent', () => {
  let component: ActivityTabsComponent;
  let fixture: ComponentFixture<ActivityTabsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivityTabsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivityTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
