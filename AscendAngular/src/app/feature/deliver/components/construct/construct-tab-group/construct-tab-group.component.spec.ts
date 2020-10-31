import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConstructTabGroupComponent } from './construct-tab-group.component';

describe('ConstructTabGroupComponent', () => {
  let component: ConstructTabGroupComponent;
  let fixture: ComponentFixture<ConstructTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConstructTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConstructTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
