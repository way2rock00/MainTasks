import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MarketplacesolutionsFilterComponent } from './marketplacesolutions-filter.component';

describe('MarketplacesolutionsFilterComponent', () => {
  let component: MarketplacesolutionsFilterComponent;
  let fixture: ComponentFixture<MarketplacesolutionsFilterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MarketplacesolutionsFilterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MarketplacesolutionsFilterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
