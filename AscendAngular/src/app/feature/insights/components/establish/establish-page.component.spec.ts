import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EstablishPageComponent } from './establish-page.component';

describe('EstablishPageComponent', () => {
  let component: EstablishPageComponent;
  let fixture: ComponentFixture<EstablishPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EstablishPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EstablishPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
