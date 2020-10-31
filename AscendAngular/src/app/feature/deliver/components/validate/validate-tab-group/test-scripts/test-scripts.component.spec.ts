import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TestScriptsComponent } from './test-scripts.component';

describe('TestScriptsComponent', () => {
  let component: TestScriptsComponent;
  let fixture: ComponentFixture<TestScriptsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TestScriptsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TestScriptsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
