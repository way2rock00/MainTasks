import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProjectDescriptionForm } from './project-description.component';

describe('ProjectDescriptionForm', () => {
  let component: ProjectDescriptionForm;
  let fixture: ComponentFixture<ProjectDescriptionForm>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProjectDescriptionForm ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProjectDescriptionForm);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
