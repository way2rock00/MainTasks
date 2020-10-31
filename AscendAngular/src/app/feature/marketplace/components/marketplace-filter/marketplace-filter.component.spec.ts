import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MarketplaceFilterComponent } from './marketplace-filter.component';

describe('MarketplaceFilterComponent', () => {
  let component: MarketplaceFilterComponent;
  let fixture: ComponentFixture<MarketplaceFilterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MarketplaceFilterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MarketplaceFilterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
