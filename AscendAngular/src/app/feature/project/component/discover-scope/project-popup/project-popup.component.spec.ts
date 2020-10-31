import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProjectPopupComponent } from './project-popup.component';

describe('ProjectPopupComponent', () => {
  let component: ProjectPopupComponent;
  let fixture: ComponentFixture<ProjectPopupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProjectPopupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProjectPopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
