import { DeployComponent } from './deploy/deploy.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';


const routes: Routes = [
  { path: '', redirectTo: 'deploy' },
  { path: 'deploy', component: DeployComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DeployTabRoutingModule { }
