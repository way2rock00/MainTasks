import { DeployComponent } from './deploy/deploy.component';
import { SharedModule } from 'src/app/shared/shared.module';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { DeployTabRoutingModule } from './deploy-tab-routing.module';


@NgModule({
  declarations: [
    DeployComponent
  ],
  imports: [
    CommonModule,
    DeployTabRoutingModule,
    SharedModule
  ]
})
export class DeployTabModule { }
