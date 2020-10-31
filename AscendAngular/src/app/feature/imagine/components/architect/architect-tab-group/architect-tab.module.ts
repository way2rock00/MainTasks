import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ArchitectTabRoutingModule } from './architect-tab-routing.module';

import { PersonasComponent } from './personas/personas.component';
import { JourneyMapsComponent } from './journey-maps/journey-maps.component';

import { SharedModule } from 'src/app/shared/shared.module';

@NgModule({
  declarations: [
    PersonasComponent,
    JourneyMapsComponent
  ],
  imports: [
    CommonModule,
    ArchitectTabRoutingModule,
    SharedModule
  ],
  providers: [ ]
})
export class ArchitectTabModule { }
