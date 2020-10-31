import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ActivitiesComponent } from './components/activities/activities.component';


const routes: Routes = [
  { path: ':route/:phaseName/:stopName', component: ActivitiesComponent },
  { path: ':route/:phaseName/:stopName/:activityId/:tabCode', component: ActivitiesComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ActivitiesRoutingModule { }
