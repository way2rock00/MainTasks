import { TestBed } from '@angular/core/testing';

import { DiscoverTabDataService } from './discover-tab-data.service';

describe('DiscoverTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DiscoverTabDataService = TestBed.get(DiscoverTabDataService);
    expect(service).toBeTruthy();
  });
});
