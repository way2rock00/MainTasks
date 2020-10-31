import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DevelopPageComponent } from './develop-page.component';

describe('DevelopPageComponent', () => {
  let component: DevelopPageComponent;
  let fixture: ComponentFixture<DevelopPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DevelopPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DevelopPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
