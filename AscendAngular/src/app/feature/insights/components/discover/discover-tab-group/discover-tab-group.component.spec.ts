import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DiscoverTabGroupComponent } from './discover-tab-group.component';

describe('DiscoverTabGroupComponent', () => {
  let component: DiscoverTabGroupComponent;
  let fixture: ComponentFixture<DiscoverTabGroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DiscoverTabGroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DiscoverTabGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
