import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DevelopmentToolsComponent } from './development-tools.component';

describe('DevelopmentToolsComponent', () => {
  let component: DevelopmentToolsComponent;
  let fixture: ComponentFixture<DevelopmentToolsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DevelopmentToolsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DevelopmentToolsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
