import { TestBed } from '@angular/core/testing';

import { DeployTabDataService } from './deploy-tab-data.service';

describe('DeployTabDataService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DeployTabDataService = TestBed.get(DeployTabDataService);
    expect(service).toBeTruthy();
  });
});
