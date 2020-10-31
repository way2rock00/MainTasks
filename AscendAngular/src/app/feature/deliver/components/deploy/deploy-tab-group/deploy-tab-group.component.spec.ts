import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DeployTabGroupComponent } from './deploy-tab-group.component';

describe('DeployTabGroupComponent', () => {
  let component: DeployTabGroupComponent;
  let fixture: ComponentFixture<DeployTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DeployTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DeployTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
