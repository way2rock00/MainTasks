import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { DefinePurposeTabRoutingModule } from './define-purpose-tab-routing.module';
import { MiscellaneousComponent } from './miscellaneous/miscellaneous.component';
import { SharedModule } from 'src/app/shared/shared.module';


@NgModule({
  declarations: [
    MiscellaneousComponent
  ],
  imports: [
    CommonModule,
    DefinePurposeTabRoutingModule,
    SharedModule
  ]
})
export class DefinePurposeTabModule { }
