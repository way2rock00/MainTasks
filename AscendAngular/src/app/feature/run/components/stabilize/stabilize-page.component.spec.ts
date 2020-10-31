import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StabilizePageComponent } from './stabilize-page.component';

describe('StabilizePageComponent', () => {
  let component: StabilizePageComponent;
  let fixture: ComponentFixture<StabilizePageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StabilizePageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StabilizePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
