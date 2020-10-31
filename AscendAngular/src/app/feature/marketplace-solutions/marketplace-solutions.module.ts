import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { MarketplaceSolutionsRoutingModule } from './marketplace-solutions-routing.module';
import { MarketplacesolutionsFilterComponent } from './components/marketplacesolutions-filter/marketplacesolutions-filter.component';
import { MarketplacesolutionsToolsComponent } from './components/marketplacesolutions-tools/marketplacesolutions-tools.component';
//import { MarketplacesolutionsComponent } from './components/marketplacesolutions/marketplacesolutions.component';
//import { MarketplacesolutionsFilterHelperComponent } from './models/marketplacesolutions-filter-helper/marketplacesolutions-filter-helper.component';

import { ThirdPartyModule } from 'src/app/shared/third-party.module';
import { ReactiveFormsModule } from '@angular/forms';
import { MarketplacesolutionsComponent } from './components/marketplacesolutions/marketplacesolutions.component';
import { MarketplacesolutionsPopupComponent } from './components/marketplacesolutions-popup/marketplacesolutions-popup.component';
import { DocumentationComponent } from './components/marketplacesolutions-popup/documentation/documentation.component';
import { ImpactComponent } from './components/marketplacesolutions-popup/impact/impact.component';
import { ProblemComponent } from './components/marketplacesolutions-popup/problem/problem.component';
import { TechStackComponent } from './components/marketplacesolutions-popup/tech-stack/tech-stack.component';

@NgModule({
  declarations: [MarketplacesolutionsFilterComponent, MarketplacesolutionsToolsComponent, MarketplacesolutionsComponent, MarketplacesolutionsPopupComponent, DocumentationComponent, ImpactComponent, ProblemComponent, TechStackComponent],//, MarketplacesolutionsFilterHelperComponent],
  imports: [
    CommonModule,ThirdPartyModule,
    ReactiveFormsModule,
    MarketplaceSolutionsRoutingModule
  ], exports: [
    MarketplacesolutionsFilterComponent
  ],
  entryComponents: [
    MarketplacesolutionsPopupComponent
  ]
})
export class MarketplaceSolutionsModule { }
