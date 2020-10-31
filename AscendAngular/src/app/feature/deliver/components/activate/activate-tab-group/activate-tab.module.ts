import { ActivateComponent } from './activate/activate.component';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ActivateTabRoutingModule } from './activate-tab-routing.module';
import { SharedModule } from 'src/app/shared/shared.module';


@NgModule({
  declarations: [
    ActivateComponent
  ],
  imports: [
    CommonModule,
    ActivateTabRoutingModule,
    SharedModule
  ]
})
export class ActivateTabModule { }
