import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { StabilizeTabRoutingModule } from './stabilize-tab-routing.module';

import { StabilizeComponent } from './stabilize/stabilize.component';
import { SharedModule } from 'src/app/shared/shared.module';


@NgModule({
  declarations: [
    StabilizeComponent
  ],
  imports: [
    CommonModule,
    StabilizeTabRoutingModule,
    SharedModule
  ]
})
export class StabilizeTabModule { }
