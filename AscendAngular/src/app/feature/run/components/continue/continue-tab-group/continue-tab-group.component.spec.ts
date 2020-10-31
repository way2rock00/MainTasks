import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ContinueTabGroupComponent } from './continue-tab-group.component';

describe('ContinueTabGroupComponent', () => {
  let component: ContinueTabGroupComponent;
  let fixture: ComponentFixture<ContinueTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ContinueTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ContinueTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
