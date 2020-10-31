import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EstimateTabGroupComponent } from './estimate-tab-group.component';

describe('EstimateTabGroupComponent', () => {
  let component: EstimateTabGroupComponent;
  let fixture: ComponentFixture<EstimateTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EstimateTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EstimateTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
