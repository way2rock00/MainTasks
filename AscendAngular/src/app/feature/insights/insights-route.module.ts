import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { CreatePageComponent } from './components/create/create-page.component';
import { DevelopPageComponent } from './components/develop/develop-page.component';
import { DiscoverPageComponent } from './components/discover/discover-page.component';
import { EstablishPageComponent } from './components/establish/establish-page.component';
import { EstimatePageComponent } from './components/estimate/estimate-page.component';
import { InsightsActivitiesPageComponent } from './components/insights-activities-page/insights-activities-page.component';

export const routes: Routes = [
  { path: 'insights-activities/:stopName', component: InsightsActivitiesPageComponent },
  { path: 'create', component: CreatePageComponent },
  { path: 'establish', component: EstablishPageComponent },
  { path: 'estimate', component: EstimatePageComponent },
  { path: 'develop', component: DevelopPageComponent },
  { path: 'discover', component: DiscoverPageComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class InsightsRoutingModule { }
