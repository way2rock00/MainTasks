import { TestBed } from '@angular/core/testing';

import { CryptUtilService } from './crypt-util.service';

describe('CryptUtilService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: CryptUtilService = TestBed.get(CryptUtilService);
    expect(service).toBeTruthy();
  });
});
