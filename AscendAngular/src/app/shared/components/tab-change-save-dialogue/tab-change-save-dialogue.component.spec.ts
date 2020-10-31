import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabChangeSaveDialogueComponent } from './tab-change-save-dialogue.component';

describe('TabChangeSaveDialogueComponent', () => {
  let component: TabChangeSaveDialogueComponent;
  let fixture: ComponentFixture<TabChangeSaveDialogueComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TabChangeSaveDialogueComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TabChangeSaveDialogueComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
