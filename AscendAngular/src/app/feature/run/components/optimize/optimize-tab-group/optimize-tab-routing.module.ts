import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AceComponent } from './ace/ace.component';
import { RegressionTestComponent } from './regression-test/regression-test.component';


const routes: Routes = [
  { path: '', redirectTo: 'regression testing' },
  { path: 'regression testing', component: RegressionTestComponent },
  { path: 'ace quarterly release insights', component: AceComponent },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class OptimizeTabRoutingModule { }
