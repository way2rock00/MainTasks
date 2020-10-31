import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateTabGroupComponent } from './create-tab-group.component';

describe('CreateTabGroupComponent', () => {
  let component: CreateTabGroupComponent;
  let fixture: ComponentFixture<CreateTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreateTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
