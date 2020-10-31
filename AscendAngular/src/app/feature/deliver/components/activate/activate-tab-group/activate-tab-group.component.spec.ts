import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivateTabGroupComponent } from './activate-tab-group.component';

describe('ActivateTabGroupComponent', () => {
  let component: ActivateTabGroupComponent;
  let fixture: ComponentFixture<ActivateTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivateTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivateTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
