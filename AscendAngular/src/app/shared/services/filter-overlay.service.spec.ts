import { TestBed } from '@angular/core/testing';

import { FilterOverlayService } from './filter-overlay.service';

describe('FilterOverlayService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: FilterOverlayService = TestBed.get(FilterOverlayService);
    expect(service).toBeTruthy();
  });
});
