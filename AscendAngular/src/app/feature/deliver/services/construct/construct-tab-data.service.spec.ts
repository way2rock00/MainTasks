import { TestBed } from '@angular/core/testing';

import { ConstructTabDataService } from './construct-tab-data.service';

describe('ConstructTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ConstructTabDataService = TestBed.get(ConstructTabDataService);
    expect(service).toBeTruthy();
  });
});
