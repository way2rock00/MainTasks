import { animate, style, transition, trigger } from '@angular/animations';
import { Location } from '@angular/common';
import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { MatDialog } from '@angular/material';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { TYPE_OF_PROJECT_PIT_TABS, TYPE_OF_PROJECT_PIT_FORM_SEGMENT } from '../../constants/project-scope-generator/project-scope-stepper';
import { ScopeGeneratorFormModel } from '../../model/project-scope-generator/scope-generator-form.model';
import { GeneratescopeService } from './../../service/generatescope.service';
import { ClientDescriptionForm } from './scope-generator-form-segment/client-description/client-description.component';
import { ProjectDescriptionForm } from './scope-generator-form-segment/project-description/project-description.component';
import { ScopeDescriptionForm } from './scope-generator-form-segment/scope-description/scope-description.component';

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
  selector: 'app-scope-generator',
  templateUrl: './scope-generator.component.html',
  styleUrls: ['./scope-generator.component.scss']
})
export class ScopeGeneratorComponent implements OnInit {

  readonly CLIENT_DESCRIPTION_TYPE: TYPE_OF_PROJECT_PIT_TABS = TYPE_OF_PROJECT_PIT_TABS.CLIENT_DESCRIPTION;
  readonly PROJECT_DESCRIPTION_TYPE: TYPE_OF_PROJECT_PIT_TABS = TYPE_OF_PROJECT_PIT_TABS.PROJECT_DESCRIPTION;

  formSegment = TYPE_OF_PROJECT_PIT_FORM_SEGMENT;
  currentPitStop: any;
  formData: ScopeGeneratorFormModel;
  postData: any[] = [];
  psgDataSubscription: Subscription;
  step = 0;

  @Input() projectType: any;
  @Input() savedProjectType: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  onInternalPitClick: EventEmitter<any> = new EventEmitter<any>();

  // @Output()
  // subNext: EventEmitter<any> = new EventEmitter<any>();

  // @Output()
  // subPrev: EventEmitter<any> = new EventEmitter<any>();

  @ViewChild('form', { static: false })
  currentFormElement: ClientDescriptionForm | ProjectDescriptionForm;

  constructor(
    private generateScope: GeneratescopeService,
    private location: Location,
    private route: ActivatedRoute,
    public dialog: MatDialog) { }
  private pageHeader: string;

  ngOnInit() {
    this.currentPitStop = this.formSegment[0];
    this.activateFormSegment(this.currentPitStop);
    // console.log('Printing Inputs');
    // console.log(this.projectType);
    // console.log(this.savedProjectType)
    this.formData = new ScopeGeneratorFormModel(this.savedProjectType);
  }

  nextClicked(clickedSegment?:any) {
    console.log('Scope generator next child click calling');
    this.currentFormElement.onNext(clickedSegment);
  }

  prevClicked() {
    console.log('Scope generator previous child click calling');
    this.currentFormElement.onPrev();
  }

  onNext(clickedSegment?:any) {
    console.log('Scope generator next click event');
    this.nextClicked(clickedSegment);
  }

  onPrev() {
    console.log('Scope generator previous click event');
    this.prevClicked();
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
      this.step = this.formSegment.indexOf(segmentToActivate);
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
    this.onInternalPitClick.emit({ currentPitStop: this.currentPitStop, formSegment: this.formSegment});   
  }

  onNextPit(form, direction = 1) {

    let indexOfCurrentPit = this.formSegment.indexOf(this.currentPitStop);
    console.log('Scope generator onNextPit:' + indexOfCurrentPit + ':' + this.formSegment.length);
    let clickedSegment = form.clickedSegment ? form.clickedSegment : null;    

    if(clickedSegment){      
      this.next.emit({postData: [this.formData], clickedSegment: clickedSegment});
    }
    else{
      if (indexOfCurrentPit == this.formSegment.length - 1) {
        this.postData.push(this.formData)
        this.next.emit(this.postData);
      } else {
        const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
        this.next.emit({postData: this.postData, nextSegment: nextSegment});
        // this.activateFormSegment(nextSegment);      
      }
    }    
  }

  onPrevPit(form, direction = -1) {
    //  this.subPrev.emit();
    let indexOfCurrentPit = this.formSegment.indexOf(this.currentPitStop);
    console.log('Scope generator onPrevPit:' + indexOfCurrentPit + ':' + this.formSegment.length);
    if (indexOfCurrentPit > 0) {
      const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
      this.activateFormSegment(nextSegment);
    }
  }

  // submit(event) {
  //   console.log('Submitted the submit event');
  //   this.postData.push({ projectType: [this.formData], geographicalScope: [], processScope: [], implementationApproach: [], phasePlanning: [] })
  //   this.next.emit(this.postData);
  // }

  isValid() {
    return this.currentFormElement.isValid();
  }

}
