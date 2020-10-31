import { DevelopmentToolsComponent } from './development-tools/development-tools.component';
import { ConversionComponent } from './conversion/conversion.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';


const routes: Routes = [
  { path: '', redirectTo: 'conversions' },
  { path: 'conversions', component: ConversionComponent },
  { path: 'development tools', component: DevelopmentToolsComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ConstructTabRoutingModule { }
