import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { DigitalDesignTabRoutingModule } from './digital-design-tab-routing.module';

import { BusinessProcessComponent } from './business-process/business-process.component';
import { UserStoriesComponent } from './user-stories/user-stories.component';
import { ErpConfigurationComponent } from './erp-configuration/erp-configuration.component';
import { BusinessSolutionComponent } from './business-solution/business-solution.component';
import { InterfacesComponent } from './interfaces/interfaces.component';
import { ReportsComponent } from './reports/reports.component';
import { KeyBusinessDecisionsComponent } from './key-business-decisions/key-business-decisions.component';

import { SharedModule } from 'src/app/shared/shared.module';

@NgModule({
  declarations: [
    BusinessSolutionComponent,
    BusinessProcessComponent,
    UserStoriesComponent,
    InterfacesComponent,
    ReportsComponent,
    ErpConfigurationComponent,
    KeyBusinessDecisionsComponent
  ],
  imports: [
    CommonModule,
    DigitalDesignTabRoutingModule,
    SharedModule
  ]
})
export class DigitalDesignTabModule { }
