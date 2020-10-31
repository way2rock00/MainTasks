import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { PersonasComponent } from './personas/personas.component';
import { JourneyMapsComponent } from './journey-maps/journey-maps.component';

const routes: Routes = [
  { path: '', redirectTo: 'personas' },
  { path: 'personas', component: PersonasComponent },
  { path: 'journey maps', component: JourneyMapsComponent },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ArchitectTabRoutingModule { }
