import { TestBed } from '@angular/core/testing';

import { LaunchJourneyTabDataService } from './launch-journey-tab-data.service';

describe('LaunchJourneyTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: LaunchJourneyTabDataService = TestBed.get(LaunchJourneyTabDataService);
    expect(service).toBeTruthy();
  });
});
