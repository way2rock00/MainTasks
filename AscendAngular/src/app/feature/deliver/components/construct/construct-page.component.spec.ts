import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConstructPageComponent } from './construct-page.component';

describe('ConstructPageComponent', () => {
  let component: ConstructPageComponent;
  let fixture: ComponentFixture<ConstructPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConstructPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConstructPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
