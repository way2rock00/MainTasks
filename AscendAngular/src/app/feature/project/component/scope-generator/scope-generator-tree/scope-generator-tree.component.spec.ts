import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeGeneratorTreeComponent } from './scope-generator-tree.component';

describe('ScopeGeneratorTreeComponent', () => {
  let component: ScopeGeneratorTreeComponent;
  let fixture: ComponentFixture<ScopeGeneratorTreeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeGeneratorTreeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeGeneratorTreeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
