import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CommonDialogueBoxComponent } from './common-dialogue-box.component';

describe('CommonDialogueBoxComponent', () => {
  let component: CommonDialogueBoxComponent;
  let fixture: ComponentFixture<CommonDialogueBoxComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CommonDialogueBoxComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CommonDialogueBoxComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
