import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MarketplaceToolsComponent } from './marketplace-tools.component';

describe('MarketplaceToolsComponent', () => {
  let component: MarketplaceToolsComponent;
  let fixture: ComponentFixture<MarketplaceToolsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MarketplaceToolsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MarketplaceToolsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
