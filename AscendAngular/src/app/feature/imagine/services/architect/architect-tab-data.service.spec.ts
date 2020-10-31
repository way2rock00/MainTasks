import { TestBed } from '@angular/core/testing';

import { ArchitectTabDataService } from './architect-tab-data.service';

describe('ArchitectTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ArchitectTabDataService = TestBed.get(ArchitectTabDataService);
    expect(service).toBeTruthy();
  });
});
