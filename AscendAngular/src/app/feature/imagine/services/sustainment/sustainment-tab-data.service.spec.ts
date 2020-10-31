import { TestBed } from '@angular/core/testing';

import { SustainmentTabDataService } from './sustainment-tab-data.service';

describe('SustainmentTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SustainmentTabDataService = TestBed.get(SustainmentTabDataService);
    expect(service).toBeTruthy();
  });
});
