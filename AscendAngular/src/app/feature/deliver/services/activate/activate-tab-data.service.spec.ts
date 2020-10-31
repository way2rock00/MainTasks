import { TestBed } from '@angular/core/testing';

import { ActivateTabDataService } from './activate-tab-data.service';

describe('ActivateTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ActivateTabDataService = TestBed.get(ActivateTabDataService);
    expect(service).toBeTruthy();
  });
});
