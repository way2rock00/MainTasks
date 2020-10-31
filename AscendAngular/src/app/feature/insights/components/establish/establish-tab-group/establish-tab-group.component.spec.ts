import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EstablishTabGroupComponent } from './establish-tab-group.component';

describe('EstablishTabGroupComponent', () => {
  let component: EstablishTabGroupComponent;
  let fixture: ComponentFixture<EstablishTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EstablishTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EstablishTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
