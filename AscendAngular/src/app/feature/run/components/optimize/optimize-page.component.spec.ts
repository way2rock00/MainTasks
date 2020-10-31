import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OptimizePageComponent } from './optimize-page.component';

describe('OptimizePageComponent', () => {
  let component: OptimizePageComponent;
  let fixture: ComponentFixture<OptimizePageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OptimizePageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OptimizePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
