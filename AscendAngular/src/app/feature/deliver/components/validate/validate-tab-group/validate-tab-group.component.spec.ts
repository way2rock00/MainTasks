import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ValidateTabGroupComponent } from './validate-tab-group.component';

describe('ValidateTabGroupComponent', () => {
  let component: ValidateTabGroupComponent;
  let fixture: ComponentFixture<ValidateTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ValidateTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ValidateTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
