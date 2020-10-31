import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from 'src/app/shared/shared.module';
import { ProjectRoutingModule } from './project-route.module'

import { CreateprojectComponent } from './component/create-project/createproject.component';
import { ProjectStepper } from './component/create-project/project-stepper/project-stepper.component';
import { ProjectDetailsForm } from './component/create-project/form-segment/project-details/project-details.component';
import { ClientDetailsForm } from './component/create-project/form-segment/client-details/client-details.component';
import { ScopeDetailsForm } from './component/create-project/form-segment/scope-details/scope-details.component';

import { ProjectMembersComponent } from './component/project-members/project-members.component';
import { ProjectSummaryComponent } from './component/project-summary/project-summary.component';
import { ProjectWorkspaceComponent } from './component/project-workspace/project-workspace.component';
import { ManageAdminComponent } from './component/project-workspace/manage-admin/manage-admin.component';

import { ProjectWorkspaceService } from './service/project-workspace.service';
import { PassProjectInfoService } from './service/pass-project-info.service';
import { CreateprojectService } from './service/createproject.service';
import { ManageAdminService } from './service/manageAdmin.service';
import { CapabilityComponent } from './component/capibility-popup/capability-popup.component';
import { SummaryFilterComponent } from './component/summary-filter/summary-filter.component';
import { ArtifactPopupComponent } from './component/artifact-popup/artifact-popup.component';
import { ScopeGeneratorComponent } from './component/scope-generator/scope-generator.component';
import { ScopeGeneratorStepperComponent } from './component/scope-generator/scope-generator-stepper/scope-generator-stepper.component';
import { ClientDescriptionForm } from './component/scope-generator/scope-generator-form-segment/client-description/client-description.component';
import { ProjectDescriptionForm } from './component/scope-generator/scope-generator-form-segment/project-description/project-description.component';
import { ScopeDescriptionForm } from './component/scope-generator/scope-generator-form-segment/scope-description/scope-description.component';
import { ProjectListComponent } from './component/discover-scope/project-list/project-list.component';
import { ProjectPopupComponent } from './component/discover-scope/project-popup/project-popup.component';
import { ProjectFormComponent } from './component/discover-scope/project-form/project-form.component';
import { ScopeFormStepperComponent } from './component/discover-scope/scope-form-stepper/scope-form-stepper.component';
import { ScopeWrapperStepperComponent } from './component/scope-generator/scope-wrapper-stepper/scope-wrapper-stepper.component';
import { GeographicScopeComponent } from './component/scope-generator/geographic-scope/geographic-scope.component';
import { ProcessScopeComponent } from './component/scope-generator/process-scope/process-scope.component';
import { ImplementationApproachFormComponent } from './component/scope-generator/implementation-approach-form/implementation-approach-form.component';
import { ScopeGeneratorTreeComponent } from './component/scope-generator/scope-generator-tree/scope-generator-tree.component';
import { PhasePlanningFormComponent } from './component/scope-generator/phase-planning-form/phase-planning-form.component';
import { TimelineComponent } from './component/scope-generator/timeline/timeline.component';
import { ScopesComponent } from './component/scope-generator/scopes/scopes.component';
import { TechnologyScopeComponent } from './component/scope-generator/technology-scope/technology-scope.component';
import { TechnologyComponent } from './component/scope-generator/technology/technology.component';
import { AssumptionsComponent } from './component/scope-generator/assumptions/assumptions.component';
import { ScopeReviewComponent } from './component/scope-generator/scope-review/scope-review.component';

@NgModule({
  declarations: [
    CreateprojectComponent,
    ProjectStepper,
    ProjectDetailsForm,
    ClientDetailsForm,
    ScopeDetailsForm,
    ProjectMembersComponent,
    ProjectSummaryComponent,
    ProjectWorkspaceComponent,
    ManageAdminComponent,
    CapabilityComponent,
    SummaryFilterComponent,
    ArtifactPopupComponent,
    ScopeGeneratorComponent,
    ScopeGeneratorStepperComponent,
    ClientDescriptionForm,
    ProjectDescriptionForm,
    ScopeDescriptionForm,
    ProjectListComponent,
    ProjectPopupComponent,
    ProjectFormComponent,
    ScopeFormStepperComponent,
    ScopeWrapperStepperComponent,
    GeographicScopeComponent,
    ProcessScopeComponent,
    ImplementationApproachFormComponent,
    ScopeGeneratorTreeComponent,
    PhasePlanningFormComponent,
    TimelineComponent
    ,ScopesComponent
    ,TechnologyScopeComponent
    ,TechnologyComponent, AssumptionsComponent, ScopeReviewComponent
  ],
  imports: [
    CommonModule,
    ProjectRoutingModule,
    SharedModule,
  ],
  providers: [
    ProjectWorkspaceService,
    PassProjectInfoService,
    CreateprojectService,
    ManageAdminService
  ],
  entryComponents: [
    ProjectMembersComponent,
    ManageAdminComponent,
    CapabilityComponent,
    ArtifactPopupComponent,
    ProjectPopupComponent,
  ]
})

export class ProjectModule { }
