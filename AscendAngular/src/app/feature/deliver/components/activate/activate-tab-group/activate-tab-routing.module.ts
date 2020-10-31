import { ActivateComponent } from './activate/activate.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';


const routes: Routes = [
  { path: '', redirectTo: 'activate digital organization' },
  { path: 'activate digital organization', component: ActivateComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ActivateTabRoutingModule { }
