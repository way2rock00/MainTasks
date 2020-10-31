import { Component, OnInit, ViewChild } from '@angular/core';
import { FlexAlignStyleBuilder } from '@angular/flex-layout';
import { MatDialog, MatSnackBar } from '@angular/material';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { CryptUtilService } from 'src/app/shared/services/crypt-util.service';
import { environment } from 'src/environments/environment';
import { SCOPE_STEPPER_FORM_SEGMENT, SCOPE_STEPPER_FORM_SEGMENT_TYPE } from '../../../constants/project-scope-generator/project-scope-stepper';
import { GeneratescopeService } from '../../../service/generatescope.service';
import { ArtifactPopupComponent } from '../../artifact-popup/artifact-popup.component';
import { AssumptionsComponent } from '../assumptions/assumptions.component';
import { ScopeGeneratorComponent } from '../scope-generator.component';
import { ScopesComponent } from '../scopes/scopes.component';
import { TechnologyScopeComponent } from '../technology-scope/technology-scope.component';
import { ImplementationApproachFormComponent } from './../implementation-approach-form/implementation-approach-form.component';


@Component({
  selector: 'app-scope-wrapper-stepper',
  templateUrl: './scope-wrapper-stepper.component.html',
  styleUrls: ['./scope-wrapper-stepper.component.scss']
})
export class ScopeWrapperStepperComponent implements OnInit {

  allPsgData: any;

  allPsgSavedData: any;

  currentPhase: string;

  @ViewChild('form1', { static: false })
  //currentFormElement: ScopeGeneratorComponent | GeographicScopeComponent | ProcessScopeComponent | ImplementationApproachFormComponent;
  currentFormElement: ScopeGeneratorComponent | ScopesComponent | TechnologyScopeComponent | ImplementationApproachFormComponent | AssumptionsComponent;
  //currentFormElement: ScopeGeneratorComponent  | ImplementationApproachFormComponent;


  formSegment = SCOPE_STEPPER_FORM_SEGMENT;
  currentPitStop: any;

  //step = 0;

  psgDataSubscription: Subscription;
  savedPsgDataSubscription: Subscription;

  projectTypeFilled: boolean = true;
  scopesFilled: boolean = true;
  implApprFilled: boolean = true;

  allPsgSavedDataBkp: any;

  projectTypeNonMandatoryFields: any[] = ['INTEGRATIONPLATFORM', 'EXTENSIONS', 'ADDITIONALERPPACKAGE', 'SECONDARYMEMBERFIRM', 'SECONDARYPORTFOLIO', 'SECONDARYOFFERING', 'ADDITIONALERPPACKAGE', 'CLIENTGROUP']

  menuOpen = false;

  nextGenLink = '';
  projectScopeLink = '';

  nextLabel: string = 'Save and Next';
  prevButton: boolean = false;

  readonly TYPE_OF_PROJECT_TYPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.TYPE_OF_PROJECT;
  readonly GENERAL_SCOPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.GENERAL_SCOPE;
  readonly TECHNICAL_SCOPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.TECHNICAL_SCOPE;
  readonly IMPLEMENTATION_APPROACH_TYPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.IMPLEMENTATION_APPROACH;
  readonly PHASE_PLANNING_TYPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.PHASE_PLANNING;
  readonly ASSUMPTIONS_TYPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.ASSUMPTIONS;
  readonly REVIEW_TYPE: SCOPE_STEPPER_FORM_SEGMENT_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.REVIEW;

  constructor(private _snackBar: MatSnackBar, private generateScope: GeneratescopeService,
    public dialog: MatDialog, private router: Router, private cryptUtilService: CryptUtilService) {
    this.nextGenLink = `${environment.BASE_URL}/projectNextGenExtract/${this.router.url.split('/')[3]}`
    this.projectScopeLink = `${environment.BASE_URL}/projectScopeDoc/${this.router.url.split('/')[3]}`
    this.psgDataSubscription = this.generateScope.fetchAllPSGData(this.router.url.split('/')[3]).subscribe(allData => {
      this.allPsgData = allData[0];

      this.savedPsgDataSubscription = this.generateScope.fetchSavedProjectData(this.router.url.split('/')[3]).subscribe(savedData => {
        this.allPsgSavedData = savedData;
        this.allPsgSavedDataBkp = JSON.parse(JSON.stringify(this.allPsgSavedData))

        // this.currentPitStop = this.formSegment[0];
        this.activateFormSegment(this.formSegment[0]);
        // this.currentPhase = this.phaseData[3].name
        this.routedStop()

      })
    })
  }

  ngOnInit() {
  }

  nextGenExtract() {
    this._snackBar.open(
      "NextGen Extract is being downloaded....",
      null,
      {
        duration: 3000
      }
    );
  }
  projectScpoeDocument() {
    this._snackBar.open(
      "Project Scope Document is being downloaded....",
      null,
      {
        duration: 3000
      }
    );
  }

  routedStop() {
    if (this.router.url.split('/')[4]) {
      if (decodeURI(this.router.url.split('/')[4]).toUpperCase() != 'ENGAGEMENT DETAILS') {
        if (this.allPsgSavedDataBkp.projectType) {
          if (this.allPsgSavedDataBkp.projectType[0]) {
            for (let i of Object.keys(this.allPsgSavedDataBkp.projectType[0])) {
              if (this.projectTypeNonMandatoryFields.indexOf(i.toUpperCase()) == -1) {
                if (this.allPsgSavedDataBkp.projectType[0][i] == null || (this.allPsgSavedDataBkp.projectType[0][i] == '' || (this.allPsgSavedDataBkp.projectType[0][i].length && this.allPsgSavedDataBkp.projectType[0][i].length == 0))) {
                  this.projectTypeFilled = false;
                  break;
                }
              }
            }
          }
        }
      }

      if (decodeURI(this.router.url.split('/')[4]).toUpperCase() != 'SCOPE' && decodeURI(this.router.url.split('/')[4]).toUpperCase() != 'ENGAGEMENT DETAILS') {
        if (this.allPsgSavedDataBkp.scopes[0].geographicalScope[0].regionCountry.length == 0 || (this.allPsgSavedDataBkp.scopes[0].processScope[0].coreBusinessProcess && this.allPsgSavedDataBkp.scopes[0].processScope[0].coreBusinessProcess.length == 0) || this.allPsgSavedDataBkp.scopes[0].systemScope.length == 0 || this.allPsgSavedDataBkp.scopes[0].serviceScope == null || this.allPsgSavedDataBkp.scopes[0].serviceScope.length == 0) {
          this.scopesFilled = false;
        }
      }

      if (decodeURI(this.router.url.split('/')[4]).toUpperCase() != 'IMPLEMENTATION APPROACH' && decodeURI(this.router.url.split('/')[4]).toUpperCase() != 'SCOPE' && decodeURI(this.router.url.split('/')[4]).toUpperCase() != 'ENGAGEMENT DETAILS') {
        if (this.allPsgSavedDataBkp.implementationApproach[0].phaseDetails.length == 0) {
          this.implApprFilled = false;
        }
      }

      if (!this.projectTypeFilled) {
        this.activateFormSegment(this.formSegment[0]);
        this._snackBar.open(
          "Some mandatory fields are missing.",
          null,
          {
            duration: 3000
          }
        );
      } else if (!this.scopesFilled) {
        this.activateFormSegment(this.formSegment[1]);
        this._snackBar.open(
          "Some mandatory fields are missing.",
          null,
          {
            duration: 3000
          }
        );
      } else if (!this.implApprFilled) {
        this.activateFormSegment(this.formSegment[2]);
        this._snackBar.open(
          "Some mandatory fields are missing.",
          null,
          {
            duration: 3000
          }
        );
      } else if (this.projectTypeFilled && this.scopesFilled && this.implApprFilled) {
        for (let i of this.formSegment) {
          if (i.label.toUpperCase() == decodeURI(this.router.url.split('/')[4]).toUpperCase()) {
            this.activateFormSegment(this.formSegment[this.formSegment.indexOf(i)])
          }
        }
      }
    }

  }

  getStopNumber() {
    return this.formSegment.indexOf(this.currentPitStop) + 1;
  }

  goto(route) {
    this.router.navigate([route + this.router.url.split('/')[3]])
  }

  scrollToTop(query) {
    const element = document.querySelector(query);
    if (element)
      element.scroll(0, 0);
  }

  pitClicked(clickedSegment){
    let diff = this.formSegment.indexOf(this.currentPitStop) - this.formSegment.indexOf(clickedSegment);
    let isPsgCompletedFlag = this.cryptUtilService.getItem('IS_PSG_COMPLETE_FLAG', 'SESSION');
    let directionOfPropagation;
    if(diff < 0)
      directionOfPropagation = 1
    else if (diff > 0)
      directionOfPropagation = -1
          
    //if moving forward, check if all intermediate pits are filled in
    if ((directionOfPropagation < 0 || isPsgCompletedFlag == 'Y') && diff != 0) {
      this.scrollToTop('.right-scope-wrapper');
      if (this.currentFormElement)
        this.currentFormElement.onNext(clickedSegment);
    }
  }

  nextClicked() {
    console.log('Main Next Clicked');
    this.scrollToTop('.right-scope-wrapper');
    if (this.currentFormElement)
      this.currentFormElement.onNext();

  }

  prevClicked() {
    console.log('Main Previous Clicked');
    console.log(this.currentFormElement);
    this.scrollToTop('.right-scope-wrapper');
    if (this.currentFormElement)
      this.currentFormElement.onPrev();
  }

  // subNext() {
  //   this.step++;
  // }

  // subPrev() {
  //   this.step--;
  // }

  async onNextPit(data, direction = 1) {
    //this.step++;
    //console.log('Main NextPit:');
    let updatedData = data.postData ? data.postData : data;
    console.log(updatedData);
    var finalPostData = [{ projectType: [] }, { scopes: [] }, { implementationApproach: [] }, { phasePlanning: [] }, { technicalScope: [] }, { assumptions: [] }];
    console.log(this.formSegment.indexOf(this.currentPitStop));
    if (this.formSegment.indexOf(this.currentPitStop) == 0)
      finalPostData[0].projectType = updatedData;
    else
      if (this.formSegment.indexOf(this.currentPitStop) == 1)
        finalPostData[this.formSegment.indexOf(this.currentPitStop)].scopes.push(updatedData);
      else
        if (this.formSegment.indexOf(this.currentPitStop) == 4)
          finalPostData[this.formSegment.indexOf(this.currentPitStop)].technicalScope.push(updatedData);
        else
          if (this.formSegment.indexOf(this.currentPitStop) == 2)
            finalPostData[this.formSegment.indexOf(this.currentPitStop)].implementationApproach.push(updatedData);
          else
            if (this.formSegment.indexOf(this.currentPitStop) == 3)
              finalPostData[this.formSegment.indexOf(this.currentPitStop)].phasePlanning = updatedData;
            else
              if (this.formSegment.indexOf(this.currentPitStop) == 5)
                finalPostData[this.formSegment.indexOf(this.currentPitStop)].assumptions = updatedData;

    // this.submit(finalPostData).then(data => {
    //   const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
    //   this.activateFormSegment(nextSegment);
    // });

    await this.submit(finalPostData);
    if(data.nextSegment){
      (this.currentFormElement as any).activateFormSegment(data.nextSegment);
    }
    else if(data.clickedSegment){
      let index = this.formSegment.findIndex( t => t == data.clickedSegment);

      if(index == -1)
        (this.currentFormElement as any).onPitClicked(data.clickedSegment);
      else
        this.onPitClicked(data.clickedSegment)
    }
    else{
      const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
      this.activateFormSegment(nextSegment);
    }
    
    // }
    // );
  }

  onPrevPit(form, direction = -1) {
    console.log('Main PreviousPit');
    const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
    this.activateFormSegment(nextSegment);
  }

  onInternalPitClick(data) {
    // this.nextLabel = data.currentPitStop != data.formSegment[data.formSegment.length - 1] ? 'Next' : 'Save and Next';
    this.prevButton = this.currentPitStop == this.formSegment[0] && data.currentPitStop == data.formSegment[0] ? false : true;
  }

  onPitClicked(clickedSegment) {
    let currIndex = this.formSegment.indexOf(this.currentPitStop);
    let nextIndex = this.formSegment.indexOf(clickedSegment);
    let diff = nextIndex - currIndex;

    let isPsgCompletedFlag = this.cryptUtilService.getItem('IS_PSG_COMPLETE_FLAG', 'SESSION');

    const directionOfPropagation =
      currIndex < nextIndex
        ? 1
        : -1;

    //if moving forward, check if all intermediate pits are filled in
    if (directionOfPropagation < 0 || isPsgCompletedFlag == 'Y') {
      //   this.activateFormSegmentInSteps(clickedSegment, directionOfPropagation);
      // } else {
      //if moving back, there is no restriction and can directly reach to destination
      this.scrollToTop('.right-scope-wrapper');
      this.activateFormSegment(clickedSegment);
      //this.step = nextIndex == 0 ? 0 : this.step + diff;--discuss with andrew
    }
  }

  //this method takes the final pit and moves one step at a time, in the process validates each step
  activateFormSegmentInSteps(finalPit, direction) {
    //if (this.currentFormElement.isValid()) {--discuss with andrew
    if (this.currentFormElement.isValid()) {
      const nextPit = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
      this.activateFormSegment(nextPit);
      //this.step = this.step < 4 ? 4 : this.step + 1;--discuss with andrew
      //this.currentFormElement.showError = false;--discuss with andrew
      //if we haven't reached at final pit, recursively call to validate next step and then move
      if (nextPit !== finalPit) {
        setTimeout(() => this.activateFormSegmentInSteps(finalPit, direction), 0);
      }
    } else {
      //submit the form to activate the error messages
      //this.currentFormElement.showError = true;--discuss with andrew
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
  
      // this.nextLabel = this.currentPitStop.children ? 'Next' : 'Save and Next';
      this.prevButton = this.currentPitStop == this.formSegment[0] ? false : true;
    }
  }

  goBack() {
    this.router.navigate(['/project/workspace'])
  }

  throwParent(phase) {
    this.currentPhase = phase;
  }

  // prev(element) {
  //   element.step--;
  // }

  // next(element) {
  //   element.nextClicked();
  // }

  // print() {
  //   console.log(this.savedData)
  // }

  async submit(finalPostData: any) {
    let self = this;
    return new Promise(
      function (resolve, reject) {
        self.generateScope.updateProjectScopeData(finalPostData, 'UPDATE', self.router.url.split('/')[3])
          .subscribe(data => {
            self.psgDataSubscription = self.generateScope.fetchAllPSGData(self.router.url.split('/')[3]).subscribe(allData => {
              self.savedPsgDataSubscription = self.generateScope.fetchSavedProjectData(self.router.url.split('/')[3]).subscribe(savedData => {
                self.allPsgData = allData[0];
                self.allPsgSavedData = savedData;
                resolve();
                // this.currentPitStop = this.formSegment[0];
                // this.activateFormSegment(this.currentPitStop);
                // this.currentPhase = this.phaseData[3].name
              })
            })
          }
          );
      }
    )
  }

  showPopup() {
    const ref = this.dialog.open(ArtifactPopupComponent, {
      data: {
        contentList: [{ tabCode: 'Process scope generator' }],
        selectedFunction: 'PSG',
      },
      height: '495px',
      width: '917px',
      panelClass: 'summaryPopupStyle',
      autoFocus: false
    });

    ref.afterClosed().subscribe(res => {
      this.router.navigate(['/project/workspace']);
    });
  }


  //need to revisit this logic.
  /*if (res.MSG == 'SUCCESS') {
    if (this.step >= 3 && this.step < 7) {
      this.savedPsgDataSubscription = this.generateScope.fetchSavedProjectData(this.router.url.split('/')[3]).subscribe(savedData => {
        this.allPsgSavedData = savedData;
      })
    }
    if (this.step >= 7) {
      this.dialog.open(CommonDialogueBoxComponent, {
        data: {
          from: 'GENERATE SCOPE',
          message: 'Successfully updated the Project Details.'
        }
      });
    }
  } else {
    this.dialog.open(CommonDialogueBoxComponent, {
      data: {
        from: '',
        message: 'Error while updating the data. Error Message: ' + res.MSG + '.'
      }
    });
  }
  });*/
  // }

  ngOnDestroy() {
    this.psgDataSubscription.unsubscribe();
    this.savedPsgDataSubscription.unsubscribe();
  }
}
