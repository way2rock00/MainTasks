import { TestBed } from '@angular/core/testing';

import { TabChangeSaveDialogueService } from './tab-change-save-dialogue.service';

describe('TabChangeSaveDialogueService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: TabChangeSaveDialogueService = TestBed.get(TabChangeSaveDialogueService);
    expect(service).toBeTruthy();
  });
});
