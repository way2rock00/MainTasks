import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArchitectActivitiesPageComponent } from './architect-activities-page.component';

describe('ArchitectActivitiesPageComponent', () => {
  let component: ArchitectActivitiesPageComponent;
  let fixture: ComponentFixture<ArchitectActivitiesPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArchitectActivitiesPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArchitectActivitiesPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
