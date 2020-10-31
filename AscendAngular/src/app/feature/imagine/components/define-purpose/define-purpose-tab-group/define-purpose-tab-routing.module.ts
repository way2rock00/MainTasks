import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { MiscellaneousComponent } from './miscellaneous/miscellaneous.component';


const routes: Routes = [
  { path: '', redirectTo: 'define digital organization' },
  { path: 'define digital organization', component: MiscellaneousComponent },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DefinePurposeTabRoutingModule { }
