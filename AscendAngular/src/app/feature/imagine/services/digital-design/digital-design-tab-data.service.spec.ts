import { TestBed } from '@angular/core/testing';

import { DigitalDesignTabDataService } from './digital-design-tab-data.service';

describe('DigitalDesignTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DigitalDesignTabDataService = TestBed.get(DigitalDesignTabDataService);
    expect(service).toBeTruthy();
  });
});
