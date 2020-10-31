import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { SharedModule } from 'src/app/shared/shared.module';
import { CreatePageComponent } from './components/create/create-page.component';
import { CreateTabGroupComponent } from './components/create/create-tab-group/create-tab-group.component';
import { DevelopPageComponent } from './components/develop/develop-page.component';
import { DevelopTabGroupComponent } from './components/develop/develop-tab-group/develop-tab-group.component';
import { DiscoverPageComponent } from './components/discover/discover-page.component';
import { DiscoverTabGroupComponent } from './components/discover/discover-tab-group/discover-tab-group.component';
import { EstablishPageComponent } from './components/establish/establish-page.component';
import { EstablishTabGroupComponent } from './components/establish/establish-tab-group/establish-tab-group.component';
import { EstimatePageComponent } from './components/estimate/estimate-page.component';
import { EstimateTabGroupComponent } from './components/estimate/estimate-tab-group/estimate-tab-group.component';
import { InsightsRoutingModule } from './insights-route.module';
import { CreateTabDataService } from './services/create/create-tab-data.service';
import { DevelopTabDataService } from './services/develop/develop-tab-data.service';
import { DiscoverTabDataService } from './services/discover/discover-tab-data.service';
import { EstablishTabDataService } from './services/establish/establish-tab-data.service';
import { EstimateTabDataService } from './services/estimate/estimate-tab-data.service';
import { LeftInsightsWheelComponent } from './components/left-insights-wheel/left-insights-wheel.component';
import { InsightsActivitiesPageComponent } from './components/insights-activities-page/insights-activities-page.component';



@NgModule({
  declarations: [
    CreatePageComponent,
    CreateTabGroupComponent,
    DevelopPageComponent,
    DiscoverPageComponent,
    EstablishPageComponent,
    EstimatePageComponent,
    DevelopTabGroupComponent,
    DiscoverTabGroupComponent,
    EstablishTabGroupComponent,
    EstimateTabGroupComponent,
    LeftInsightsWheelComponent,
    InsightsActivitiesPageComponent
  ],
  imports: [
    CommonModule,
    InsightsRoutingModule,
    SharedModule
  ],
  providers: [
    CreateTabDataService,
    DiscoverTabDataService,
    EstimateTabDataService,
    DevelopTabDataService,
    EstablishTabDataService
  ]
})
export class InsightsModule { }
