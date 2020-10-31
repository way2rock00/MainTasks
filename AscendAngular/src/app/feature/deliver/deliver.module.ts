import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { SharedModule } from 'src/app/shared/shared.module';
/*  -- ACTIVATE -- */
import { ActivatePageComponent } from './components/activate/activate-page.component';
import { ActivateTabGroupComponent } from './components/activate/activate-tab-group/activate-tab-group.component';
/* -- CONSTRUCT -- */
import { ConstructPageComponent } from './components/construct/construct-page.component';
import { ConstructTabGroupComponent } from './components/construct/construct-tab-group/construct-tab-group.component';
import { ConversionComponent } from './components/construct/construct-tab-group/conversion/conversion.component';
import { DevelopmentToolsComponent } from './components/construct/construct-tab-group/development-tools/development-tools.component';
/* -- DEPLOY -- */
import { DeployPageComponent } from './components/deploy/deploy-page.component';
import { DeployTabGroupComponent } from './components/deploy/deploy-tab-group/deploy-tab-group.component';
import { DeployComponent } from './components/deploy/deploy-tab-group/deploy/deploy.component';
/* -- VALIDATE -- */
import { ValidatePageComponent } from './components/validate/validate-page.component';
import { TestAutomationComponent } from './components/validate/validate-tab-group/test-automation/test-automation.component';
import { TestScriptsComponent } from './components/validate/validate-tab-group/test-scripts/test-scripts.component';
import { ValidateTabGroupComponent } from './components/validate/validate-tab-group/validate-tab-group.component';
import { DeliverRoutingModule } from './deliver-route.module';
import { ActivateTabDataService } from './services/activate/activate-tab-data.service';
/* -- SERVICES -- */
import { ConstructTabDataService } from './services/construct/construct-tab-data.service';
import { DeployTabDataService } from './services/deploy/deploy-tab-data.service';
import { ValidateTabDataService } from './services/validate/validate-tab-data.service';











@NgModule({
  declarations: [
    ConstructPageComponent,
    ConstructTabGroupComponent,
    // ConversionComponent,
    // DevelopmentToolsComponent,
    ValidatePageComponent,
    ValidateTabGroupComponent,
    // TestAutomationComponent,
    // TestScriptsComponent,
    DeployPageComponent,
    ActivatePageComponent,
    ActivateTabGroupComponent,
    DeployTabGroupComponent,
    // ActivateComponent,
    // DeployComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    DeliverRoutingModule
  ],
  providers: [
    ConstructTabDataService,
    ValidateTabDataService,
    ActivateTabDataService,
    DeployTabDataService
  ],
  entryComponents : []
})
export class DeliverModule { }
