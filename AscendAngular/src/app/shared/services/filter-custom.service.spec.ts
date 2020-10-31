import { TestBed } from '@angular/core/testing';

import { FilterCustomService } from './filter-custom.service';

describe('FilterCustomService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: FilterCustomService = TestBed.get(FilterCustomService);
    expect(service).toBeTruthy();
  });
});
