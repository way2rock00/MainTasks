import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivatePageComponent } from './activate-page.component';

describe('ActivatePageComponent', () => {
  let component: ActivatePageComponent;
  let fixture: ComponentFixture<ActivatePageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivatePageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivatePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
