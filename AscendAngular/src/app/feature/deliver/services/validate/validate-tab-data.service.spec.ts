import { TestBed } from '@angular/core/testing';

import { ValidateTabDataService } from './validate-tab-data.service';

describe('ValidateTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ValidateTabDataService = TestBed.get(ValidateTabDataService);
    expect(service).toBeTruthy();
  });
});
