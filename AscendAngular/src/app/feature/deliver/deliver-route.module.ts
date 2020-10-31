import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ActivatePageComponent } from './components/activate/activate-page.component';
import { ConstructPageComponent } from './components/construct/construct-page.component';
import { DeployPageComponent } from './components/deploy/deploy-page.component';
import { ValidatePageComponent } from './components/validate/validate-page.component';



export const routes: Routes = [
    // { path: 'construct/:tab', component: ConstructPageComponent },
    { path: '', redirectTo: 'activate'},

    // { path: 'construct', component: ConstructPageComponent, loadChildren: './components/construct/construct-tab-group/construct-tab.module#ConstructTabModule' },
    { path: 'construct', redirectTo: 'construct/conversions'},
    { path: 'construct/conversions', component: ConstructPageComponent},
    { path: 'construct/development tools', component: ConstructPageComponent},
    { path: 'construct/deliverables', component: ConstructPageComponent},
    { path: 'constructocm/construct ocm', component: ConstructPageComponent},

    // { path: 'validate', component: ValidatePageComponent, loadChildren: './components/validate/validate-tab-group/validate-tab.module#ValidateTabModule' },
    { path: 'validate', redirectTo: 'validate/test scenarios and scripts' },
    { path: 'validate/test scenarios and scripts', component: ValidatePageComponent},
    { path: 'validate/test automations', component: ValidatePageComponent},
    { path: 'validate/deliverables', component: ValidatePageComponent},
    { path: 'validateocm/validate ocm', component: ValidatePageComponent},
    
    // { path: 'deploy', component: DeployPageComponent, loadChildren: './components/deploy/deploy-tab-group/deploy-tab.module#DeployTabModule' },
    { path: 'deploy', redirectTo: 'deploy/deliverables' },
    { path: 'deploy/deliverables', component: DeployPageComponent},
    { path: 'deployocm/deploy ocm', component: DeployPageComponent},

    // { path: 'activate', component:ActivatePageComponent, loadChildren: './components/activate/activate-tab-group/activate-tab.module#ActivateTabModule'}
    { path: 'activate', redirectTo: 'activate/deliverables'},
    { path: 'activate/deliverables', component: ActivatePageComponent },
    { path: 'activatedigitalorganizationocm/activate digital organization ocm', component: ActivatePageComponent }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class DeliverRoutingModule { }
