import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ToolsBarComponent } from './tools-bar.component';

describe('ToolsBarComponent', () => {
  let component: ToolsBarComponent;
  let fixture: ComponentFixture<ToolsBarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ToolsBarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ToolsBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
