import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MarketplacesolutionsToolsComponent } from './marketplacesolutions-tools.component';

describe('MarketplacesolutionsToolsComponent', () => {
  let component: MarketplacesolutionsToolsComponent;
  let fixture: ComponentFixture<MarketplacesolutionsToolsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MarketplacesolutionsToolsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MarketplacesolutionsToolsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
