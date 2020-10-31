import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProcessScopeComponent } from './process-scope.component';

describe('ProcessScopeComponent', () => {
  let component: ProcessScopeComponent;
  let fixture: ComponentFixture<ProcessScopeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProcessScopeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProcessScopeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
