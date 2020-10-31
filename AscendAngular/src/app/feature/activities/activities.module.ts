import { SharedModule } from './../../shared/shared.module';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ActivitiesRoutingModule } from './activities-routing.module';
import { ActivitiesComponent } from './components/activities/activities.component';
import { ThirdPartyModule } from 'src/app/shared/third-party.module';
import { ActivitiesFilterComponent } from './components/activities-filter/activities-filter.component';
import { ActivitiesDetailsComponent } from './components/activities-details/activities-details.component';
import { ActivityTabsComponent } from './components/activity-tabs/activity-tabs.component';


@NgModule({
  declarations: [ActivitiesComponent, ActivitiesFilterComponent, ActivitiesDetailsComponent, ActivityTabsComponent],
  imports: [
    CommonModule,
    ThirdPartyModule,
    ActivitiesRoutingModule,
    SharedModule
  ]
})
export class ActivitiesModule { }
