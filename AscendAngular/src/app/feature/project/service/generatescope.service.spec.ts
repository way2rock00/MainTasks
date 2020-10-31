import { TestBed } from '@angular/core/testing';

import { GeneratescopeService } from './generatescope.service';

describe('GeneratescopeService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: GeneratescopeService = TestBed.get(GeneratescopeService);
    expect(service).toBeTruthy();
  });
});
