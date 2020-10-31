import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DigitalDesignPageComponent } from './digital-design-page.component';

describe('DigitalDesignPageComponent', () => {
  let component: DigitalDesignPageComponent;
  let fixture: ComponentFixture<DigitalDesignPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DigitalDesignPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DigitalDesignPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
