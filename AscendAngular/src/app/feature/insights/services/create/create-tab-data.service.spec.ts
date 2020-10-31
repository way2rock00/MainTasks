import { TestBed } from '@angular/core/testing';

import { CreateTabDataService } from './create-tab-data.service';

describe('CreateTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: CreateTabDataService = TestBed.get(CreateTabDataService);
    expect(service).toBeTruthy();
  });
});
