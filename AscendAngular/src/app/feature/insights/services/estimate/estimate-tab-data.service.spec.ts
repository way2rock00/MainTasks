import { TestBed } from '@angular/core/testing';

import { EstimateTabDataService } from './estimate-tab-data.service';

describe('EstimateTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: EstimateTabDataService = TestBed.get(EstimateTabDataService);
    expect(service).toBeTruthy();
  });
});
