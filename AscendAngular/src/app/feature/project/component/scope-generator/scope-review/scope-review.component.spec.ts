import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScopeReviewComponent } from './scope-review.component';

describe('ScopeReviewComponent', () => {
  let component: ScopeReviewComponent;
  let fixture: ComponentFixture<ScopeReviewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScopeReviewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScopeReviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
