import { TestBed } from '@angular/core/testing';

import { ContinueTabDataService } from './continue-tab-data.service';

describe('ContinueTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ContinueTabDataService = TestBed.get(ContinueTabDataService);
    expect(service).toBeTruthy();
  });
});
