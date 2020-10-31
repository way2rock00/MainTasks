import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { OptimizeTabRoutingModule } from './optimize-tab-routing.module';

import { AceComponent } from './ace/ace.component';
import { RegressionTestComponent } from './regression-test/regression-test.component';

import { SharedModule } from 'src/app/shared/shared.module';

@NgModule({
  declarations: [
    AceComponent,
    RegressionTestComponent
  ],
  imports: [
    CommonModule,
    OptimizeTabRoutingModule,
    SharedModule
  ]
})
export class OptimizeTabModule { }
