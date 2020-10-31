import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConfigWorkbooksComponent } from './config-workbooks.component';

describe('ConfigWorkbooksComponent', () => {
  let component: ConfigWorkbooksComponent;
  let fixture: ComponentFixture<ConfigWorkbooksComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConfigWorkbooksComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigWorkbooksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
