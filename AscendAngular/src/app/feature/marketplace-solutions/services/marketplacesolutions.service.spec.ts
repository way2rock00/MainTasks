import { TestBed } from '@angular/core/testing';

import { MarketplaceSolutionsService } from './marketplacesolutions.service';

describe('MarketplaceSolutionsService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: MarketplaceSolutionsService = TestBed.get(MarketplaceSolutionsService);
    expect(service).toBeTruthy();
  });
});