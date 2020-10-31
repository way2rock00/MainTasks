import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DigitalMaturityComponent } from './digital-maturity.component';

describe('DigitalMaturityComponent', () => {
  let component: DigitalMaturityComponent;
  let fixture: ComponentFixture<DigitalMaturityComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DigitalMaturityComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DigitalMaturityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
