import { MarketingMaterialsComponent } from './shared/components/marketing-materials/marketing-materials.component';
import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { FourOOneComponent } from './shared/components/401/four-o-one.component';
import { FourOFourComponent } from './shared/components/404/four-o-four.component';
import { AboutComponent } from './shared/components/about/about.component';
import { TutorialsComponent } from './shared/components/tutorials/tutorials.component';
import { IsAuthenticatedGuard } from './shared/services/gaurds/can-activate/isAuthenticated.guard';

const routes: Routes = [
  { path: '', loadChildren: './base/base.module#BaseModule' },
  { path: 'project', canActivate: [IsAuthenticatedGuard], loadChildren: './feature/project/project.module#ProjectModule' },
  { path: 'imagine', loadChildren: './feature/imagine/imagine.module#ImagineModule' },
  { path: 'deliver', loadChildren: './feature/deliver/deliver.module#DeliverModule' },
  { path: 'run', loadChildren: './feature/run/run.module#RunModule' },
  { path: 'insights', loadChildren: './feature/insights/insights.module#InsightsModule' },
  { path: 'marketplace', loadChildren: './feature/marketplace/marketplace.module#MarketplaceModule' },
  { path: 'marketplace-solutions', loadChildren: './feature/marketplace-solutions/marketplace-solutions.module#MarketplaceSolutionsModule' }, 
  { path: 'activities', loadChildren: './feature/activities/activities.module#ActivitiesModule' },
  { path: 'about', component: AboutComponent },
  { path: 'tutorials', component: TutorialsComponent },
  { path: 'marketing', component: MarketingMaterialsComponent },
  { path: 'unauthorized', component: FourOOneComponent },
  { path: '**', component: FourOFourComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
