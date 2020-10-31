import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArchitectPageComponent } from './architect-page.component';

describe('ArchitectPageComponent', () => {
  let component: ArchitectPageComponent;
  let fixture: ComponentFixture<ArchitectPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArchitectPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArchitectPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
