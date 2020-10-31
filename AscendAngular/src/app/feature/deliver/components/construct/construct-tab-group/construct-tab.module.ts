import { SharedModule } from 'src/app/shared/shared.module';
import { DevelopmentToolsComponent } from './development-tools/development-tools.component';
import { ConversionComponent } from './conversion/conversion.component';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ConstructTabRoutingModule } from './construct-tab-routing.module';


@NgModule({
  declarations: [
    ConversionComponent,
    DevelopmentToolsComponent
  ],
  imports: [
    CommonModule,
    ConstructTabRoutingModule,
    SharedModule
  ]
})
export class ConstructTabModule { }
