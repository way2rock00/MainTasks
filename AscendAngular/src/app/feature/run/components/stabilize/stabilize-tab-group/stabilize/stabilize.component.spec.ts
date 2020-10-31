import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StabilizeComponent } from './stabilize.component';

describe('StabilizeComponent', () => {
  let component: StabilizeComponent;
  let fixture: ComponentFixture<StabilizeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StabilizeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StabilizeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
