import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DefinePurposePageComponent } from './define-purpose-page.component';

describe('DefinePurposePageComponent', () => {
  let component: DefinePurposePageComponent;
  let fixture: ComponentFixture<DefinePurposePageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DefinePurposePageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DefinePurposePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
