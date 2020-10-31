import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DevelopTabGroupComponent } from './develop-tab-group.component';

describe('DevelopTabGroupComponent', () => {
  let component: DevelopTabGroupComponent;
  let fixture: ComponentFixture<DevelopTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DevelopTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DevelopTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
