import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { MarketplaceRoutingModule } from './marketplace-routing.module';

import { MarketplaceComponent } from './components/marketplace/marketplace.component';
import { AmplifierPopupComponent } from './components/amplifier-popup/amplifier-popup.component';
import { MarketplaceFilterComponent } from './components/marketplace-filter/marketplace-filter.component';
import { MarketplaceToolsComponent } from './components/marketplace-tools/marketplace-tools.component';

import { ThirdPartyModule } from 'src/app/shared/third-party.module';
import { ReactiveFormsModule } from '@angular/forms';
import { DocumentationComponent } from './components/amplifier-popup/documentation/documentation.component';
import { ImpactComponent } from './components/amplifier-popup/impact/impact.component';
import { ProblemComponent } from './components/amplifier-popup/problem/problem.component';
import { TechStackComponent } from './components/amplifier-popup/tech-stack/tech-stack.component';

@NgModule({
  declarations: [
    MarketplaceComponent,
    MarketplaceFilterComponent,
    MarketplaceToolsComponent,
    AmplifierPopupComponent,
    DocumentationComponent,
    ImpactComponent,
    ProblemComponent,
    TechStackComponent
  ],
  imports: [
    CommonModule,
    ThirdPartyModule,
    ReactiveFormsModule,
    MarketplaceRoutingModule
  ],
  exports: [
    MarketplaceFilterComponent
  ],
  entryComponents: [
    AmplifierPopupComponent
  ]
})
export class MarketplaceModule { }
