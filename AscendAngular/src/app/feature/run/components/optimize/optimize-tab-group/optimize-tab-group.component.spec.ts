import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OptimizeTabGroupComponent } from './optimize-tab-group.component';

describe('OptimizeTabGroupComponent', () => {
  let component: OptimizeTabGroupComponent;
  let fixture: ComponentFixture<OptimizeTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OptimizeTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OptimizeTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
