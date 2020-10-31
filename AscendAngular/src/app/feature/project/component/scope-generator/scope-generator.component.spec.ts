import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeGeneratorComponent } from './scope-generator.component';

describe('ScopeGeneratorComponent', () => {
  let component: ScopeGeneratorComponent;
  let fixture: ComponentFixture<ScopeGeneratorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeGeneratorComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeGeneratorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
