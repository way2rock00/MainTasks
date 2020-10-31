import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { StabilizeComponent } from './stabilize/stabilize.component';


const routes: Routes = [
  { path: '', redirectTo: 'stabilize' },
  { path: 'stabilize', component: StabilizeComponent },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class StabilizeTabRoutingModule { }
