import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { RunRoutingModule } from './run-routing.module';
import { SharedModule } from 'src/app/shared/shared.module';

/* -- OPTIMIZE -- */
import { OptimizePageComponent } from './components/optimize/optimize-page.component';
import { OptimizeTabGroupComponent } from './components/optimize/optimize-tab-group/optimize-tab-group.component';

/* STABILIZE */
import { StabilizePageComponent } from './components/stabilize/stabilize-page.component';
import { StabilizeTabGroupComponent } from './components/stabilize/stabilize-tab-group/stabilize-tab-group.component';

/* CONTINUE */
import { ContinuePageComponent } from './components/continue/continue-page.component';
import { ContinueTabGroupComponent } from './components/continue/continue-tab-group/continue-tab-group.component';


/* -- SERVICES -- */
import { OptimizeTabDataService } from './services/optimize/optimize-tab-data.service';
import { ContinueTabDataService } from './services/continue/continue-tab-data.service';
import { StabilizeTabDataService } from './services/stabilize/stabilize-tab-data.service';

@NgModule({
  declarations: [
    OptimizePageComponent,
    OptimizeTabGroupComponent,
    StabilizePageComponent,
    StabilizeTabGroupComponent,
    ContinuePageComponent,
    ContinueTabGroupComponent
  ],
  imports: [
    CommonModule,
    RunRoutingModule,
    SharedModule
  ],
  providers: [
    OptimizeTabDataService,
    StabilizeTabDataService,
    ContinueTabDataService
  ],
  entryComponents: [  ]
})
export class RunModule { }
