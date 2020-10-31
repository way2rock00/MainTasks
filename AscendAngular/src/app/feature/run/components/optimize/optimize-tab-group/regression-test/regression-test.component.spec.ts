import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RegressionTestComponent } from './regression-test.component';

describe('RegressionTestComponent', () => {
  let component: RegressionTestComponent;
  let fixture: ComponentFixture<RegressionTestComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RegressionTestComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RegressionTestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
