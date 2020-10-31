import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ToolsBarPopupComponent } from './tools-bar-popup.component';

describe('ToolsBarPopupComponent', () => {
  let component: ToolsBarPopupComponent;
  let fixture: ComponentFixture<ToolsBarPopupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ToolsBarPopupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ToolsBarPopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
