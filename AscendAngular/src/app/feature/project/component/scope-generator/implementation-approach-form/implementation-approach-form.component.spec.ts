import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImplementationApproachFormComponent } from './implementation-approach-form.component';

describe('ImplementationApproachFormComponent', () => {
  let component: ImplementationApproachFormComponent;
  let fixture: ComponentFixture<ImplementationApproachFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImplementationApproachFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImplementationApproachFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
