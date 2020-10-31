import { TestBed } from '@angular/core/testing';

import { DefinePurposeTabDataService } from './define-purpose-tab-data.service';

describe('DefinePurposeTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DefinePurposeTabDataService = TestBed.get(DefinePurposeTabDataService);
    expect(service).toBeTruthy();
  });
});
