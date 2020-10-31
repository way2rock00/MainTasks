import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { KeyBusinessDecisionsComponent } from './key-business-decisions.component';

describe('KeyBusinessDecisionsComponent', () => {
  let component: KeyBusinessDecisionsComponent;
  let fixture: ComponentFixture<KeyBusinessDecisionsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ KeyBusinessDecisionsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(KeyBusinessDecisionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
