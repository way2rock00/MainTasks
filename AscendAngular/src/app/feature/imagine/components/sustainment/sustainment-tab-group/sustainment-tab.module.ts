import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SustainmentTabRoutingModule } from './sustainment-tab-routing.module';

import { DeliverablesComponent } from './deliverables/deliverables.component';
import { UserStoryLibraryComponent } from './user-story-library/user-story-library.component';
import { ConfigWorkbooksComponent } from './config-workbooks/config-workbooks.component';

import { SharedModule } from 'src/app/shared/shared.module';

@NgModule({
  declarations: [
    DeliverablesComponent,
    UserStoryLibraryComponent,
    ConfigWorkbooksComponent
  ],
  imports: [
    CommonModule,
    SustainmentTabRoutingModule,
    SharedModule
  ]
})
export class SustainmentTabModule { }
