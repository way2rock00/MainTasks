import { TestBed } from '@angular/core/testing';

import { ToolsBarService } from './tools-bar.service';

describe('ToolsBarService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: ToolsBarService = TestBed.get(ToolsBarService);
    expect(service).toBeTruthy();
  });
});
