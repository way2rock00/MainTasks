import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ImagineRoutingModule } from './imagine-route.module';
import { SharedModule } from 'src/app/shared/shared.module';

import { LeftImagineWheelComponent } from './components/left-imagine-wheel/left-imagine-wheel.component';

/* -- ARCHITECT -- */
import { ArchitectPageComponent } from './components/architect/architect-page.component';
import { ArchitectTabGroupComponent } from './components/architect/architect-tab-group/tab-group.component';

/* -- DESIGN -- */
import { DigitalDesignPageComponent } from './components/digital-design/digital-design-page.component';
import { DigitalDesignTabGroupComponent } from './components/digital-design/digital-design-tab-group/digital-design-tab-group.component'

import { ArchitectActivitiesPageComponent } from './components/architect/architect-activities-page/architect-activities-page.component';

/* SUSTAINMENT */
import { SustainmentPageComponent } from './components/sustainment/sustainment-page.component';
import { SustainmentTabGroupComponent } from './components/sustainment/sustainment-tab-group/sustainment-tab-group.component';

/* DEFINE DIGITAL ORGANIZATION */
import { DefinePurposePageComponent } from './components/define-purpose/define-purpose-page.component';
import { DefinePurposeTabGroupComponent } from './components/define-purpose/define-purpose-tab-group/define-purpose-tab-group.component';

/* LAUNCH JOURNEY */ 
import { LaunchJourneyPageComponent } from './components/launch-journey/launch-journey-page.component';
import { LaunchJourneyTabGroupComponent } from './components/launch-journey/launch-journey-tab-group/launch-journey-tab-group.component';


/* -- SERVICES -- */
import { ArchitectTabDataService } from './services/architect/architect-tab-data.service'
import { DigitalDesignTabDataService } from './services/digital-design/digital-design-tab-data.service';
import { DefinePurposeTabDataService } from './services/define-purpose/define-purpose-tab-data.service';
import { SustainmentTabDataService } from './services/sustainment/sustainment-tab-data.service';
import { DeliverablesComponent } from './components/digital-design/digital-design-tab-group/deliverables/deliverables.component';


@NgModule({
  declarations: [

    ArchitectPageComponent,
    ArchitectTabGroupComponent,
    DigitalDesignPageComponent,
    DigitalDesignTabGroupComponent,
    SustainmentPageComponent,
    SustainmentTabGroupComponent,
    DefinePurposePageComponent,
    DefinePurposeTabGroupComponent,
    LaunchJourneyPageComponent,
    LaunchJourneyTabGroupComponent,
    LeftImagineWheelComponent,
    ArchitectActivitiesPageComponent,
    DeliverablesComponent //tab changes
  ],
  imports: [
    CommonModule,
    ImagineRoutingModule,
    SharedModule
  ],
  providers: [
    ArchitectTabDataService,
    DigitalDesignTabDataService,
    DefinePurposeTabDataService,
    SustainmentTabDataService   
  ],
  entryComponents: [ ]
})
export class ImagineModule { }
