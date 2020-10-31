import { ContinuePageComponent } from './components/continue/continue-page.component';
import { StabilizePageComponent } from './components/stabilize/stabilize-page.component';
import { OptimizePageComponent } from './components/optimize/optimize-page.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';


export const routes: Routes = [
  { path: '', redirectTo: 'stabilize'},
  // { path: 'optimize', component: OptimizePageComponent, loadChildren: './components/optimize/optimize-tab-group/optimize-tab.module#OptimizeTabModule' },
  { path: 'optimize', redirectTo: 'optimize/deliverables' },
  { path: 'optimize/deliverables', component: OptimizePageComponent },

  // { path: 'stabilize', component: StabilizePageComponent, loadChildren: './components/stabilize/stabilize-tab-group/stabilize-tab.module#StabilizeTabModule'},
  { path: 'stabilize', redirectTo: 'stabilize/deliverables'},
  { path: 'stabilize/deliverables', component: StabilizePageComponent},

  { path: 'continue', redirectTo: 'continuedigitalorganization/deliverables'},
  { path: 'continuedigitalorganization/deliverables', component: ContinuePageComponent}
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class RunRoutingModule { }
