import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ContinuePageComponent } from './continue-page.component';

describe('ContinuePageComponent', () => {
  let component: ContinuePageComponent;
  let fixture: ComponentFixture<ContinuePageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ContinuePageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ContinuePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
