import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MarketplacesolutionsComponent } from './marketplacesolutions.component';

describe('MarketplacesolutionsComponent', () => {
  let component: MarketplacesolutionsComponent;
  let fixture: ComponentFixture<MarketplacesolutionsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MarketplacesolutionsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MarketplacesolutionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
