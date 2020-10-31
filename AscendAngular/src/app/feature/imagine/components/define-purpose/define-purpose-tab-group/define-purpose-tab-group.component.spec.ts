import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DefinePurposeTabGroupComponent } from './define-purpose-tab-group.component';

describe('DefinePurposeTabGroupComponent', () => {
  let component: DefinePurposeTabGroupComponent;
  let fixture: ComponentFixture<DefinePurposeTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DefinePurposeTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DefinePurposeTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
