import { LaunchJourneyPageComponent } from './components/launch-journey/launch-journey-page.component';
import { DefinePurposePageComponent } from './components/define-purpose/define-purpose-page.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { DigitalDesignPageComponent } from './components/digital-design/digital-design-page.component';
import { ArchitectPageComponent } from './components/architect/architect-page.component';
import { SustainmentPageComponent } from './components/sustainment/sustainment-page.component';

export const routes: Routes = [
    { path: '', redirectTo: 'launch' },

    // { path: 'digital-design', component: DigitalDesignPageComponent, loadChildren: './components/digital-design/digital-design-tab-group/digital-design-tab.module#DigitalDesignTabModule' },
    { path: 'digital-design', redirectTo: 'digital-design/business solutions' },
    { path: 'digital-design/process flows', component: DigitalDesignPageComponent },
    { path: 'digital-design/user stories', component: DigitalDesignPageComponent },
    { path: 'digital-design/erp configurations', component: DigitalDesignPageComponent },
    { path: 'digital-design/business solutions', component: DigitalDesignPageComponent },
    { path: 'digital-design/interfaces', component: DigitalDesignPageComponent },
    { path: 'digital-design/analytics & reports', component: DigitalDesignPageComponent },
    { path: 'digital-design/key design decisions', component: DigitalDesignPageComponent },

    // { path: 'architect', component: ArchitectPageComponent, loadChildren: './components/architect/architect-tab-group/architect-tab.module#ArchitectTabModule' },
    { path: 'architect', redirectTo: 'architect/personas' },
    { path: 'architect/personas', component: ArchitectPageComponent },
    { path: 'architect/journey maps', component: ArchitectPageComponent },

    // { path: 'refineuserstories', component: SustainmentPageComponent, loadChildren: './components/sustainment/sustainment-tab-group/sustainment-tab.module#SustainmentTabModule' },
    { path: 'refineuserstories', redirectTo: 'refineuserstories/deliverables'},
    { path: 'refineuserstories/deliverables', component: SustainmentPageComponent },
    { path: 'refineuserstories/user story library', component: SustainmentPageComponent },
    { path: 'refineuserstories/configuration workbooks', component: SustainmentPageComponent },
    { path: 'refineuserstoriesocm/refine user stories ocm', component: SustainmentPageComponent },
    
    // { path: 'definedigitalorganization', component: DefinePurposePageComponent, loadChildren: './components/define-purpose/define-purpose-tab-group/define-purpose-tab.module#DefinePurposeTabModule' },
    { path: 'definedigitalorganization', redirectTo: 'definedigitalorganization/define digital organization' },
    { path: 'definedigitalorganization/define digital organization', component: DefinePurposePageComponent },

    { path: 'launch', redirectTo: 'launchjourney/deliverables' },
    { path: 'launchjourney/deliverables', component: LaunchJourneyPageComponent },
    { path: 'launchjourneyocm/launch journey ocm', component: LaunchJourneyPageComponent },
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class ImagineRoutingModule { }
