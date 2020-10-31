import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TestAutomationComponent } from './test-automation.component';

describe('TestAutomationComponent', () => {
  let component: TestAutomationComponent;
  let fixture: ComponentFixture<TestAutomationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TestAutomationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TestAutomationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
