import { Component, OnInit, ViewChild, Input, EventEmitter, Output } from '@angular/core';
import { ScopeGeneratorTreeComponent } from '../scope-generator-tree/scope-generator-tree.component';
import { SCOPE_PIT_TABS, SCOPE_PIT_FORM_SEGMENT } from '../../../constants/project-scope-generator/project-scope-stepper';
import { trigger, transition, animate, style } from '@angular/animations';
import { TechnologyComponent } from '../technology/technology.component';


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
  selector: 'app-scopes',
  templateUrl: './scopes.component.html',
  styleUrls: ['./scopes.component.scss']
})
export class ScopesComponent implements OnInit {

  @ViewChild('form', { static: false })
  currentFormElement: TechnologyComponent;

  @Input()
  savedScopePitData: any;

  @Input()
  scopePitOptionsData: any;

  readonly GEOGRAPHIC_SCOPE: SCOPE_PIT_TABS = SCOPE_PIT_TABS.GEOGRAPHIC_SCOPE;
  readonly PROCESS_SCOPE: SCOPE_PIT_TABS = SCOPE_PIT_TABS.PROCESS_SCOPE;
  readonly SYSTEM_SCOPE: SCOPE_PIT_TABS = SCOPE_PIT_TABS.SYSTEM_SCOPE;
  readonly SERVICE_SCOPE: SCOPE_PIT_TABS = SCOPE_PIT_TABS.SERVICE_SCOPE;

  formSegment = SCOPE_PIT_FORM_SEGMENT;
  currentPitStop: any;

  // @Output()
  // subNext: EventEmitter<any> = new EventEmitter<any>();

  // @Output()
  // subPrev: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  onInternalPitClick: EventEmitter<any> = new EventEmitter<any>();

  constructor() { }

  ngOnInit() {
    this.currentPitStop = this.formSegment[0];
    this.activateFormSegment(this.currentPitStop);
    console.log('Printing Inputs');
    console.log(this.savedScopePitData);
    console.log(this.scopePitOptionsData);
  }

  getTreeData(stopType) {
    if (stopType == this.GEOGRAPHIC_SCOPE)
      return this.scopePitOptionsData.geographicalScope[0].regionCountry;
    else if (stopType == this.PROCESS_SCOPE)
      return this.scopePitOptionsData.processScope[0].coreBusinessProcess;
    else if (stopType == this.SYSTEM_SCOPE)
      return this.scopePitOptionsData;
    else if (stopType == this.SERVICE_SCOPE)
      return this.scopePitOptionsData.serviceScope[0];
  }

  getSelectedData(stopType) {
    if (stopType == this.GEOGRAPHIC_SCOPE)
      return this.savedScopePitData.geographicalScope[0].regionCountry;
    else if (stopType == this.PROCESS_SCOPE)
      return this.savedScopePitData.processScope[0].coreBusinessProcess;
    else if (stopType == this.SYSTEM_SCOPE)
      return this.savedScopePitData.systemScope[0];
    else if (stopType == this.SERVICE_SCOPE)
      return this.savedScopePitData.serviceScope;
  }

  nextClicked(clickedSegment?: any) {
    console.log('Scope landing nextClicked');
    this.currentFormElement.onNext(clickedSegment);
  }

  prevClicked() {
    console.log('Scope landing prevClicked');
    this.currentFormElement.onPrev();
  }

  onNext(clickedSegment?: any) {
    console.log('Scope landing onNext');
    this.nextClicked(clickedSegment);
  }

  onPrev() {
    console.log('Scope landing onPrev');
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

  isValid() {
    return this.currentFormElement.isValid()
  }

  //this method takes the final pit and moves one step at a time, in the process validates each step
  activateFormSegmentInSteps(finalPit, direction) {
    if (this.isValid()) {
      const nextPit = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
      this.activateFormSegment(nextPit)
      //if we haven't reached at final pit, recursively call to validate next step and then move
      if (nextPit !== finalPit) {
        setTimeout(() => this.activateFormSegmentInSteps(finalPit, direction), 0);
      }
    } else {
      //submit the form to activate the error messages
      //this.currentFormElement.ngForm.onSubmit(null);
      this.currentFormElement.onSubmit();
    }

  }

  activateFormSegment(segmentToActivate) {
    if (segmentToActivate) {
      let segmentCrossed = !!~this.formSegment.indexOf(segmentToActivate);
      //this.step = this.formSegment.indexOf(segmentToActivate);
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
    // this.onInternalPitClick.emit({ currentPitStop: this.currentPitStop, formSegment: this.formSegment});
  }

  onNextPit(updatedData, direction = 1) {
    console.log('Printing in the main file: Stop Is:' + this.currentPitStop.label)
    console.log(updatedData);


    let clickedSegment = updatedData.clickedSegment ? updatedData.clickedSegment : null;
    updatedData = clickedSegment ? updatedData.data : updatedData;

    //this.subNext.emit();
    let indexOfCurrentPit = this.formSegment.indexOf(this.currentPitStop);
    console.log('Scope landing onNextPit:' + indexOfCurrentPit + ':' + this.formSegment.length);

    if (this.currentPitStop.label == this.GEOGRAPHIC_SCOPE)
      this.savedScopePitData.geographicalScope[0].regionCountry = updatedData;
    else if (this.currentPitStop.label == this.PROCESS_SCOPE)
      this.savedScopePitData.processScope[0].coreBusinessProcess = updatedData;
    else if (this.currentPitStop.label == this.SYSTEM_SCOPE)
      this.savedScopePitData.systemScope = updatedData;
    else if (this.currentPitStop.label == this.SERVICE_SCOPE)
      this.savedScopePitData.serviceScope = updatedData;

    if(clickedSegment){
      this.next.emit({postData: this.savedScopePitData, clickedSegment: clickedSegment});
    }

    else{
      if (indexOfCurrentPit == this.formSegment.length - 1) {
        this.next.emit(this.savedScopePitData);
      } else {
        //this.subNext.emit();
        console.log('Printing current pit values on next:' + indexOfCurrentPit + ':' + this.formSegment.length)
        const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
        this.next.emit({postData: this.savedScopePitData, nextSegment: nextSegment});
        // this.activateFormSegment(nextSegment);
        console.log(nextSegment);    
      }
    }
    
  }

  onPrevPit(form, direction = -1) {
    //this.subPrev.emit();
    let indexOfCurrentPit = this.formSegment.indexOf(this.currentPitStop);
    console.log('Scope landing onPrevPit:' + indexOfCurrentPit + ':' + this.formSegment.length);
    if (indexOfCurrentPit == 0) {
      this.prev.emit();
    } else {
      //this.subPrev.emit();
      console.log('Printing current pit values on prev:' + indexOfCurrentPit + ':' + this.formSegment.length)
      const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
      this.activateFormSegment(nextSegment);
      console.log(nextSegment);
    }
  }


}
