import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AmplifiersComponent } from './amplifiers.component';

describe('AmplifiersComponent', () => {
  let component: AmplifiersComponent;
  let fixture: ComponentFixture<AmplifiersComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AmplifiersComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AmplifiersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
