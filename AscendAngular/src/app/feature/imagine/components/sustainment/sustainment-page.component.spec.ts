import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SustainmentPageComponent } from './sustainment-page.component';

describe('SustainmentPageComponent', () => {
  let component: SustainmentPageComponent;
  let fixture: ComponentFixture<SustainmentPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SustainmentPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SustainmentPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
