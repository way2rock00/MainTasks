import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MarketplacesolutionsPopupComponent } from './marketplacesolutions-popup.component';

describe('MarketplacesolutionsPopupComponent', () => {
  let component: MarketplacesolutionsPopupComponent;
  let fixture: ComponentFixture<MarketplacesolutionsPopupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MarketplacesolutionsPopupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MarketplacesolutionsPopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
