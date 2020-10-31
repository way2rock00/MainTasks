import { TestBed } from '@angular/core/testing';

import { OptimizeTabDataService } from './optimize-tab-data.service';

describe('OptimizeTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: OptimizeTabDataService = TestBed.get(OptimizeTabDataService);
    expect(service).toBeTruthy();
  });
});
