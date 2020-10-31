import { DigitalMaturityComponent } from './components/digital-maturity/digital-maturity.component';
import { AmplifiersComponent } from './components/amplifiers/amplifiers.component';
import { ActivityHeadComponent } from './components/activity-head/activity-head.component';
import { LeftNavRunComponent } from './components/standard-view-layout/layout-left/left-nav-run/left-nav-run.component';
import { NgModule, ModuleWithProviders, Optional, SkipSelf, APP_INITIALIZER, ValueProvider, Provider } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

/* -- MATERIAL MODULE -- */
import { ThirdPartyModule } from './third-party.module';

/* -- SHARED COMPONENTS -- */
import { MenuBarComponent } from './components/menu-bar/menu-bar.component';
import { NavigationBarComponent} from './components/navigation-bar/navigation-bar.component';
import { NavSearchComponent } from './components/nav-search/nav-search.component';
import { FourOFourComponent } from './components/404/four-o-four.component';
import { FourOOneComponent } from './components/401/four-o-one.component';
import { StandardViewLayoutComponent } from './components/standard-view-layout/standard-view-layout.component';
import { LayoutLeftComponent } from './components/standard-view-layout/layout-left/layout-left.component';
import { LayoutRightComponent } from './components/standard-view-layout/layout-right/layout-right.component';
import { ToolsBarComponent } from './components/tools-bar/tools-bar.component';
import { AboutComponent } from './components/about/about.component';

import { FilterBarComponent } from './components/filter-bar/filter-bar.component';
import { FilterOverlayComponent } from './components/filter-overlay/filter-overlay.component';
import { FilterSearchComponent } from './components/filter-search/filter-search.component';

import { LeftNavDeliverComponent } from './components/standard-view-layout/layout-left/left-nav-deliver/left-nav-deliver.component';
import { LeftNavImagineComponent } from './components/standard-view-layout/layout-left/left-nav-imagine/left-nav-imagine.component';
import { LeftNavInsightsComponent } from './components/standard-view-layout/layout-left/left-nav-insights/left-nav-insights.component';

import { MegaMenuComponent } from './components/mega-menu/mega-menu.component';
import { CommonDialogueBoxComponent } from './components/common-dialogue-box/common-dialogue-box.component';
import { MarketingMaterialsComponent } from './components/marketing-materials/marketing-materials.component';
import { TutorialsComponent } from './components/tutorials/tutorials.component';
import { VideoPlayerComponent } from './components/video-player/video-player.component';
import { TabContentComponent } from './components/tab-content/tab-content.component';

import { InfoPopupComponent } from './components/info-popup/info-popup.component';
import { InfoImagineWheelComponent } from './components/info-popup/info-imagine-wheel/info-imagine-wheel.component';
import { InfoInsightsWheelComponent } from './components/info-popup/info-insights-wheel/info-insights-wheel.component';
import { InfoDeliverWheelComponent } from './components/info-popup/info-deliver-wheel/info-deliver-wheel.component';
import { InfoRunWheelComponent } from './components/info-popup/info-run-wheel/info-run-wheel.component';

import { ToolsBarPopupComponent } from './components/tools-bar-popup/tools-bar-popup.component';
import { ProblemComponent } from './components/tools-bar-popup/problem/problem.component';
import { TechStackComponent } from './components/tools-bar-popup/tech-stack/tech-stack.component';
import { ImpactComponent } from './components/tools-bar-popup/impact/impact.component';
import { DocumentationComponent } from './components/tools-bar-popup/documentation/documentation.component';
import { TabChangeSaveDialogueComponent } from './components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { ActivityDeliverablesComponent } from './components/activity-deliverables/activity-deliverables.component';

/* -- SHARED PIPES -- */
import { SearchIndustryPipe } from './pipes/search-industry.pipe';
import { SecureImagePipe } from './pipes/secure-image.pipe';
import { HighlightTextPipe } from './pipes/highlight-text.pipe';

/* -- SHARED SERVICES -- */
import { FilterOverlayService} from './services/filter-overlay.service';
import { FilterOverlay } from './components/filter-overlay/filter-overlay.service';
import { ToolsBarService } from './services/tools-bar.service';

/* -- GUARD SERVICES -- */
import { IsAuthenticatedGuard } from './services/gaurds/can-activate/isAuthenticated.guard';
import { IsAdminGuard } from './services/gaurds/can-activate/isAdmin.guard';
import { IsProjectAdminGuard } from './services/gaurds/can-activate/isProjectAdmin.guard';

/* -- SINGLETON SERVICES -- */
import { MessagingService } from './services/messaging.service';
import { PassGlobalInfoService } from './services/pass-project-global-info.service';
import { SharedService } from './services/shared.service';
import { AuthenticationService } from './services/authentication.service';
import { RouterModule } from '@angular/router';
import { UtilService } from './services/util.service';
import { TabChangeSaveDialogueService } from './services/tab-change-save-dialogue.service';
import { UrlHelperService } from './services/url-helper.service';

/* -- INTERCEPTORS -- */
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import { MsalInterceptor } from '@azure/msal-angular';
import { LoaderInterceptor } from './services/http-interceptor/http-interceptor.service';
import { AuthTokenInterceptor } from './services/http-interceptor/auth-token.interceptor.service';
import { FilterContentComponent } from './components/filter-custom/filter-content/filter-content.component';
import { FilterListComponent } from './components/filter-custom/filter-list/filter-list.component';
import { FilterCustomService } from './services/filter-custom.service';
import { TabBodyComponent } from './components/tab-body/tab-body.component';
import { TabDetailsComponent } from './components/tab-details/tab-details.component';


//If you want your service to share data amongst all modules, place them here
const SingletonServices: Provider[] = [
    HttpClientModule,
    PassGlobalInfoService,
    SharedService,
    MessagingService,
    AuthenticationService,
    UtilService
];

@NgModule({
    imports: [
        FormsModule,
        ReactiveFormsModule,
        ThirdPartyModule,
        CommonModule,
        RouterModule
    ],
    declarations: [
        NavigationBarComponent,
        NavSearchComponent,
        FourOFourComponent,
        FourOOneComponent,
        StandardViewLayoutComponent,
        LayoutLeftComponent,
        LayoutRightComponent,
        FilterBarComponent,
        FilterOverlayComponent,
        FilterSearchComponent,
        ToolsBarComponent,
        MenuBarComponent,
        LeftNavDeliverComponent,
        LeftNavImagineComponent,
        LeftNavRunComponent,
        AboutComponent,
        ActivityDeliverablesComponent,
        ActivityHeadComponent,
        DigitalMaturityComponent,
        AmplifiersComponent,
        LeftNavInsightsComponent,
        ActivityHeadComponent,
        ActivityDeliverablesComponent,
        AmplifiersComponent,
        DigitalMaturityComponent,
        InfoPopupComponent,
        InfoImagineWheelComponent,
        InfoInsightsWheelComponent,
        InfoDeliverWheelComponent,
        InfoRunWheelComponent,
        ToolsBarPopupComponent,
        ProblemComponent,
        TechStackComponent,
        ImpactComponent,
        DocumentationComponent,
        TabChangeSaveDialogueComponent,
        TabContentComponent,
        FilterContentComponent,
        FilterListComponent,
        TabBodyComponent,
        TabDetailsComponent,

        /* -- PIPES-- */
        SearchIndustryPipe,
        SecureImagePipe,
        HighlightTextPipe,
        MegaMenuComponent,
        CommonDialogueBoxComponent,
        MarketingMaterialsComponent,
        TutorialsComponent,
        VideoPlayerComponent,
        
        ],
    exports: [
        FormsModule,
        ReactiveFormsModule,
        ThirdPartyModule,

        /* -- SHARED COMPONENT EXPORT -- */
        NavigationBarComponent,
        FourOFourComponent,
        StandardViewLayoutComponent,
        FilterBarComponent,
        FilterOverlayComponent,
        FilterSearchComponent,
        ToolsBarComponent,
        MenuBarComponent,
        AboutComponent,
        ActivityDeliverablesComponent,
        LeftNavInsightsComponent,
        ActivityHeadComponent,
        ActivityDeliverablesComponent,
        AmplifiersComponent,
        DigitalMaturityComponent,
        MegaMenuComponent,
        TabContentComponent,
        FilterContentComponent,
        FilterListComponent,
        TabBodyComponent,
        TabDetailsComponent,
        
        /* -- PIPES-- */
        SearchIndustryPipe,
        HighlightTextPipe,
        SecureImagePipe
    ],
    providers: [
        UrlHelperService,
        FilterOverlayService,
        FilterCustomService,
        FilterOverlay,
        ToolsBarService,
        IsAuthenticatedGuard,
        IsAdminGuard,
        IsProjectAdminGuard,
        UtilService,
        TabChangeSaveDialogueService,
        FilterCustomService, 
        // { provide: HTTP_INTERCEPTORS, useClass: LoaderInterceptor, multi: true },
        // { provide: HTTP_INTERCEPTORS, useClass: MsalInterceptor, multi: true },
        // { provide: HTTP_INTERCEPTORS, useClass: AuthTokenInterceptor, multi: true}
    ],
    entryComponents: [
        FilterOverlayComponent,
        FilterSearchComponent,
        FilterContentComponent,
        InfoPopupComponent,
        ToolsBarPopupComponent,
        TabChangeSaveDialogueComponent,
        MegaMenuComponent,
        CommonDialogueBoxComponent,
        VideoPlayerComponent
    ]
})

export class SharedModule {
    static forRoot(): ModuleWithProviders {
        return {
            ngModule: SharedModule,
            providers: SingletonServices
        };
    }
}
