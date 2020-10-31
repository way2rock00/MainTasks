import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SustainmentTabGroupComponent } from './sustainment-tab-group.component';

describe('SustainmentTabGroupComponent', () => {
  let component: SustainmentTabGroupComponent;
  let fixture: ComponentFixture<SustainmentTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SustainmentTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SustainmentTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
