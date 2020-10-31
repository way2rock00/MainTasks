import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserStoryLibraryComponent } from './user-story-library.component';

describe('UserStoryLibraryComponent', () => {
  let component: UserStoryLibraryComponent;
  let fixture: ComponentFixture<UserStoryLibraryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserStoryLibraryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserStoryLibraryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
