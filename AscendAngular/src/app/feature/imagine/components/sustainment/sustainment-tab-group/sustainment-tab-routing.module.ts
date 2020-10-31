import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { DeliverablesComponent } from './deliverables/deliverables.component';
import { UserStoryLibraryComponent } from './user-story-library/user-story-library.component';
import { ConfigWorkbooksComponent } from './config-workbooks/config-workbooks.component';


const routes: Routes = [
  { path: '', redirectTo: 'deliverables' },
  { path: 'deliverables', component: DeliverablesComponent },
  { path: 'user story library', component: UserStoryLibraryComponent },
  { path: 'configuration workbooks', component: ConfigWorkbooksComponent },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class SustainmentTabRoutingModule { }
