import { TestBed } from '@angular/core/testing';

import { EstablishTabDataService } from './establish-tab-data.service';

describe('EstablishTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: EstablishTabDataService = TestBed.get(EstablishTabDataService);
    expect(service).toBeTruthy();
  });
});
