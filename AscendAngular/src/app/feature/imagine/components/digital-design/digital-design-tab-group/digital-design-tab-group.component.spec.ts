import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DigitalDesignTabGroupComponent } from './digital-design-tab-group.component';

describe('DigitalDesignTabGroupComponent', () => {
  let component: DigitalDesignTabGroupComponent;
  let fixture: ComponentFixture<DigitalDesignTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DigitalDesignTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DigitalDesignTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
