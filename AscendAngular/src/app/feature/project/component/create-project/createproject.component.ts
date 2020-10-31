import { CommonDialogueBoxComponent } from './../../../../shared/components/common-dialogue-box/common-dialogue-box.component';
import { Component, OnInit, ViewChild, OnDestroy } from '@angular/core';
import { trigger, style, transition, animate } from '@angular/animations';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { ProjectFormModel } from '../../model/add-edit-project/project-form.model';
import { ProjectDetailsModel } from '../../model/add-edit-project/project-details.model';
import { ClientDetailsModel } from '../../model/add-edit-project/client-details.model';
import { ScopeDetailsModel } from '../../model/add-edit-project/scope-details.model';
import { ProjectDetailsForm } from './form-segment/project-details/project-details.component';
import { ClientDetailsForm } from './form-segment/client-details/client-details.component';
import { ScopeDetailsForm } from './form-segment/scope-details/scope-details.component';
import { CreateprojectService } from '../../service/createproject.service';
import { PROJECT_FORM_SEGMENT, PROJECT_FORM_SEGMENT_TYPE, PROJECT_API_ACTIONS } from '../../constants/project-common';
import { Location } from '@angular/common';
import { MatDialog } from '@angular/material';

@Component({
  animations: [
    trigger(
      'enterAnimation', [
      transition(':enter', [
        style({ opacity: 0 }),
        animate('300ms', style({ opacity: 1 }))
      ]),
      transition(':leave', [
        style({ opacity: 1 }),
        animate('300ms', style({ opacity: 0 }))
      ])
    ]
    )
  ],
  selector: 'app-createproject',
  templateUrl: './createproject.component.html',
  styleUrls: ['./createproject.component.scss']
})

export class CreateprojectComponent implements OnInit, OnDestroy {
  readonly PROJECT_DETAILS_TYPE: PROJECT_FORM_SEGMENT_TYPE = PROJECT_FORM_SEGMENT_TYPE.PROJECT_DETAILS;
  readonly CLIENT_DETAILS_TYPE: PROJECT_FORM_SEGMENT_TYPE = PROJECT_FORM_SEGMENT_TYPE.CLIENT_DETAILS;
  readonly SCOPE_DETAILS_TYPE: PROJECT_FORM_SEGMENT_TYPE = PROJECT_FORM_SEGMENT_TYPE.SCOPE_DETAILS;

  formSegment = PROJECT_FORM_SEGMENT;
  currentPitStop: any;
  formData: ProjectFormModel;
  routeSubcription$: Subscription;

  @ViewChild('form', { static: false })
  currentFormElement: ProjectDetailsForm | ClientDetailsForm | ScopeDetailsForm;

  constructor(
    private createProject: CreateprojectService,
    private location: Location,
    private route: ActivatedRoute,
    public dialog: MatDialog) { }
  private pageHeader: string;

  ngOnInit() {
    this.currentPitStop = this.formSegment[0];
    this.activateFormSegment(this.currentPitStop);
    this.routeSubcription$ = this.route.params.subscribe(data => {
      this.fetchSavedProjectDetails(data.projectId);
    });
  }

  fetchSavedProjectDetails(projectId: String) {
    //console.log('projectId:'+projectId)
    if (projectId) {
      this.createProject.fetchSavedProjectData(projectId)
        .subscribe(savedData => {
          console.log('******Fetching Project related information******')
          console.log(savedData)
          this.formData = new ProjectFormModel(savedData, projectId);
          this.pageHeader = 'Modify project';
        });
    } else {
      console.log("Create project");      
      this.formData = new ProjectFormModel();
      this.pageHeader = 'Create project'
    }
  }

  onPitClicked(clickedSegment) {
    const directionOfPropagation =
      this.formSegment.indexOf(this.currentPitStop) < this.formSegment.indexOf(clickedSegment)
        ? 1
        : -1;

    //if moving forward, check if all intermediate pits are filled in
    if (directionOfPropagation > 0) {
      this.activateFormSegmentInSteps(clickedSegment, directionOfPropagation);
    } else {
      //if moving back, there is no restriction and can directly reach to destination
      this.activateFormSegment(clickedSegment);
    }
  }

  //this method takes the final pit and moves one step at a time, in the process validates each step
  activateFormSegmentInSteps(finalPit, direction) {
    if (this.currentFormElement.isValid()) {

      const nextPit = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];

      this.activateFormSegment(nextPit)
      //if we haven't reached at final pit, recursively call to validate next step and then move
      if (nextPit !== finalPit) {
        setTimeout(() => this.activateFormSegmentInSteps(finalPit, direction), 0);
      }
    } else {
      //submit the form to activate the error messages
      this.currentFormElement.ngForm.onSubmit(null);
    }

  }

  activateFormSegment(segmentToActivate) {
    if (segmentToActivate) {
      let segmentCrossed = !!~this.formSegment.indexOf(segmentToActivate);
      this.formSegment.forEach((segment) => {
        if (segmentToActivate === segment) {
          this.currentPitStop = segmentToActivate;
          segment.active = true;
          segment.crossed = false;
          segmentCrossed = false;
        } else if (segmentCrossed) {
          segment.crossed = true;
          segment.active = false;
        } else {
          segment.active = false;
          segment.crossed = false;
        }
      });
    }
  }

  onNextPit(formData: ProjectDetailsModel | ClientDetailsModel | ScopeDetailsModel, direction = 1) {
    const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
    this.activateFormSegment(nextSegment);
  }

  submit(event) {
    // console.log(this.formData.isEdit);
    let action = '';
    if (this.formData.isEdit)
      action = PROJECT_API_ACTIONS.UPDATE;
    else
      action = PROJECT_API_ACTIONS.CREATE;
    this.createProject.updateProjectData(this.formData, action)
      .subscribe(data => {
        let res: any = data;
        // console.log('Response Msg:'+res.MSG);

        if (res.MSG == 'SUCCESS') {
          // alert('Successfully updated the Project Details');
          // this.location.back();
          this.dialog.open(CommonDialogueBoxComponent, {
            data: {
              from: 'CREATE PROJECT',
              message: 'Successfully updated the Project Details.'
            }
          });
        }
        else {
          // alert('Error while updating the data. Error Message:' + res.MSG);
          this.dialog.open(CommonDialogueBoxComponent, {
            data: {
              from: 'CREATE PROJECT',
              message: 'Error while updating the data. Error Message: ' + res.MSG + '.'
            }
          });
        }
        //// console.log('Confirmation saved:');
      });
  }

  ngOnDestroy() {
    this.routeSubcription$.unsubscribe();
  }

  goBack() {
    this.location.back();
  }

}
