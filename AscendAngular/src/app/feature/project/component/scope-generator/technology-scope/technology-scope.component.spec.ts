import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TechnologyScopeComponent } from './technology-scope.component';

describe('TechnologyScopeComponent', () => {
  let component: TechnologyScopeComponent;
  let fixture: ComponentFixture<TechnologyScopeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TechnologyScopeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TechnologyScopeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
