import { TestBed } from '@angular/core/testing';

import { DevelopTabDataService } from './develop-tab-data.service';

describe('DevelopTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DevelopTabDataService = TestBed.get(DevelopTabDataService);
    expect(service).toBeTruthy();
  });
});
