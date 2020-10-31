import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ClientDescriptionForm } from './client-description.component';

describe('ClientDescriptionForm', () => {
  let component: ClientDescriptionForm;
  let fixture: ComponentFixture<ClientDescriptionForm>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ClientDescriptionForm ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ClientDescriptionForm);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
