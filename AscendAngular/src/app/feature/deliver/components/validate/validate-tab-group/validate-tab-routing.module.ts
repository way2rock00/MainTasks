import { TestAutomationComponent } from './test-automation/test-automation.component';
import { TestScriptsComponent } from './test-scripts/test-scripts.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';


const routes: Routes = [
  { path: '', redirectTo: 'test scenarios and scripts' },
  { path: 'test automations', component: TestAutomationComponent },
  { path: 'test scenarios and scripts', component: TestScriptsComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ValidateTabRoutingModule { }
