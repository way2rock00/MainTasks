import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';

import { BaseRoutingModule } from './base-route.module';

import { HomeComponent } from './components/home/home.component';
import { HomeCarouselcardComponent } from './components/home/home-carouselcard/home-carouselcard.component';
import { HomeDescriptionComponent } from './components/home/home-description/home-description.component';

import { WelcomeComponent } from './components/welcome/welcome.component';
import { UserSelectionComponent } from './components/user-selection/user-selection.component';
import { RunComponent } from './components/home/home-carouselcard/run/run.component';
import { DeliverComponent } from './components/home/home-carouselcard/deliver/deliver.component';
import { ImagineComponent } from './components/home/home-carouselcard/imagine/imagine.component';
import { InsightsComponent } from './components/home/home-carouselcard/insights/insights.component';

@NgModule({
  declarations: [
    HomeComponent,
    HomeCarouselcardComponent,
    HomeDescriptionComponent,
    WelcomeComponent,
    UserSelectionComponent,
    RunComponent,
    DeliverComponent,
    ImagineComponent,
    InsightsComponent
  ],
  imports: [
    CommonModule,
    BaseRoutingModule,
    SharedModule
  ],
  providers: []
})
export class BaseModule { }
