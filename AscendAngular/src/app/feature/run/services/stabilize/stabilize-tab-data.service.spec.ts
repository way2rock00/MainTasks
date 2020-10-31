import { TestBed } from '@angular/core/testing';

import { StabilizeTabDataService } from './stabilize-tab-data.service';

describe('StabilizeTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: StabilizeTabDataService = TestBed.get(StabilizeTabDataService);
    expect(service).toBeTruthy();
  });
});
