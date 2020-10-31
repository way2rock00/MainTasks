import { ScopeReviewComponent } from './component/scope-generator/scope-review/scope-review.component';
import { TimelineComponent } from './component/scope-generator/timeline/timeline.component';
import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { IsAdminGuard } from 'src/app/shared/services/gaurds/can-activate/isAdmin.guard';
import { IsProjectAdminGuard } from 'src/app/shared/services/gaurds/can-activate/isProjectAdmin.guard';
import { CreateprojectComponent } from './component/create-project/createproject.component';
import { ProjectFormComponent } from './component/discover-scope/project-form/project-form.component';
import { ProjectListComponent } from './component/discover-scope/project-list/project-list.component';
import { ScopeFormStepperComponent } from './component/discover-scope/scope-form-stepper/scope-form-stepper.component';
import { ProjectSummaryComponent } from './component/project-summary/project-summary.component';
import { ProjectWorkspaceComponent } from './component/project-workspace/project-workspace.component';
import { ScopeWrapperStepperComponent } from './component/scope-generator/scope-wrapper-stepper/scope-wrapper-stepper.component';

export const routes: Routes = [
  { path: 'summary', component: ProjectSummaryComponent },
  { path: 'workspace', component: ProjectWorkspaceComponent },
  { path: 'create', canActivate: [IsAdminGuard], component: CreateprojectComponent },
  { path: 'update/:projectId', canActivate: [IsProjectAdminGuard], component: CreateprojectComponent },
  { path: 'list', component: ProjectListComponent },
  { path: 'modify', component: ProjectFormComponent },
  { path: 'psg/:projectId', component: ScopeWrapperStepperComponent },
  { path: 'stepper', component: ScopeFormStepperComponent },
  { path: 'timeline', component: TimelineComponent },  
  { path: 'psg/:projectId/:stop', component: ScopeWrapperStepperComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ProjectRoutingModule { }
