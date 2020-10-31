import { TestAutomationComponent } from './test-automation/test-automation.component';
import { TestScriptsComponent } from './test-scripts/test-scripts.component';
import { SharedModule } from 'src/app/shared/shared.module';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ValidateTabRoutingModule } from './validate-tab-routing.module';


@NgModule({
  declarations: [
    TestScriptsComponent,
    TestAutomationComponent
  ],
  imports: [
    CommonModule,
    ValidateTabRoutingModule,
    SharedModule
  ]
})
export class ValidateTabModule { }
