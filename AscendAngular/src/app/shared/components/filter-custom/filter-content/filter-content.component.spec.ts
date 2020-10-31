import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FilterContentComponent } from './filter-content.component';

describe('FilterContentComponent', () => {
  let component: FilterContentComponent;
  let fixture: ComponentFixture<FilterContentComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FilterContentComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FilterContentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
