import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArtifactPopupComponent } from './artifact-popup.component';

describe('ArtifactPopupComponent', () => {
  let component: ArtifactPopupComponent;
  let fixture: ComponentFixture<ArtifactPopupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArtifactPopupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArtifactPopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
