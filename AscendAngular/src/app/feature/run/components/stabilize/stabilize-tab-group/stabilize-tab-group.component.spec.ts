import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StabilizeTabGroupComponent } from './stabilize-tab-group.component';

describe('StabilizeTabGroupComponent', () => {
  let component: StabilizeTabGroupComponent;
  let fixture: ComponentFixture<StabilizeTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StabilizeTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StabilizeTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
