import { Component, OnInit, Input, Output, EventEmitter, ViewChild } from '@angular/core';
import { TECHNOLOGY_SCOPE_PIT_TABS, TECHNOLOGY_SCOPE_PIT_FORM_SEGMENT, SYSTEM_SCOPE_LABEL } from '../../../constants/project-scope-generator/project-scope-stepper';

import { Subscription } from 'rxjs';
import { TechnologyComponent } from '../technology/technology.component';
import { trigger, transition, style, animate } from '@angular/animations';
import { GeneratescopeService } from '../../../service/generatescope.service';

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
  selector: 'app-technology-scope',
  templateUrl: './technology-scope.component.html',
  styleUrls: ['./technology-scope.component.scss']
})
export class TechnologyScopeComponent implements OnInit {

  readonly CONVERSION_SCOPE: TECHNOLOGY_SCOPE_PIT_TABS = TECHNOLOGY_SCOPE_PIT_TABS.CONVERSION_SCOPE;
  readonly REPORTS_SCOPE: TECHNOLOGY_SCOPE_PIT_TABS = TECHNOLOGY_SCOPE_PIT_TABS.REPORTS_SCOPE;
  readonly INTERFACES_SCOPE: TECHNOLOGY_SCOPE_PIT_TABS = TECHNOLOGY_SCOPE_PIT_TABS.INTERFACES_SCOPE;
  readonly EXTENSION_SCOPE: TECHNOLOGY_SCOPE_PIT_TABS = TECHNOLOGY_SCOPE_PIT_TABS.EXTENSION_SCOPE;

  formSegment = TECHNOLOGY_SCOPE_PIT_FORM_SEGMENT;
  //formData: ScopeGeneratorFormModel;  -- This needs to be changed
  currentPitStop: any;
  postData: any[] = [];
  projectId: any;
  treeLevel: string = '3';

  @Input() technologyScopePitOptionsData: any;
  @Input() savedTechnologyScopePitData: any;
  @Input() savedSystemScopeData: any;

  treeSavedSystemScopeData: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();


  @ViewChild('form', { static: false })
  currentFormElement: TechnologyComponent;

  @Output()
  onInternalPitClick: EventEmitter<any> = new EventEmitter<any>();

  constructor(private service: GeneratescopeService) { }

  ngOnInit() {
    this.currentPitStop = this.formSegment[0];
    this.activateFormSegment(this.currentPitStop);
    this.treeSavedSystemScopeData = this.service.formatData(SYSTEM_SCOPE_LABEL, this.savedSystemScopeData)
    //Need to change this to get the actual data.
    //this.formData = new ScopeGeneratorFormModel(this.savedProjectType, this.savedProjectType.projectid);
    //this.projectId = this.savedProjectType.projectId;

  }

  getTreeData(stopType) {
    if (stopType == this.CONVERSION_SCOPE)
      return this.technologyScopePitOptionsData.conversion;
    else if (stopType == this.REPORTS_SCOPE)
      return this.technologyScopePitOptionsData.reports;
    else if (stopType == this.INTERFACES_SCOPE)
      return this.technologyScopePitOptionsData.interfaces;
    else if (stopType == this.EXTENSION_SCOPE)
      return this.technologyScopePitOptionsData.extensions;

  }

  getSelectedData(stopType) {
    if (stopType == this.CONVERSION_SCOPE && this.savedTechnologyScopePitData)
      return this.savedTechnologyScopePitData.conversion;
    else if (stopType == this.REPORTS_SCOPE && this.savedTechnologyScopePitData)
      return this.savedTechnologyScopePitData.reports;
    else if (stopType == this.INTERFACES_SCOPE && this.savedTechnologyScopePitData)
      return this.savedTechnologyScopePitData.interfaces;
    else if (stopType == this.EXTENSION_SCOPE && this.savedTechnologyScopePitData)
      return this.savedTechnologyScopePitData.extensions;

  }

  nextClicked(clickedSegment?: any) {
    console.log('Technology Click Track 2');
    this.currentFormElement.onNext(clickedSegment);
  }

  prevClicked() {
    this.currentFormElement.onPrev();
  }

  onNext(clickedSegment?: any) {
    console.log('Technology Tile Main:onNext');
    this.nextClicked(clickedSegment);
  }

  onPrev() {
    console.log('Technology Tile Main:onPrev');
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
      //this.currentFormElement.onSubmit();
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
    // this.onInternalPitClick.emit({ currentPitStop: this.currentPitStop, formSegment: this.formSegment});
  }

  updateAllData(allData, updateNode) {
    for (let i of allData) {
      if (i.children) {
        if (i.item == updateNode.item) {
          for (let j of updateNode.children) {
            if (j.new) {
              // if ('selected' in j) {
              //   delete j.selected;
              // }
              if ('new' in j) {
                delete j.new;
              }
              i.children.push(j)
            }
          }
        } else {
          this.updateAllData(i.children, updateNode);
        }
      }
    }
  }

  onNextPit(updatedData, direction = 1) {
    //this.subNext.emit();
    let indexOfCurrentPit = this.formSegment.indexOf(this.currentPitStop);
    let clickedSegment = updatedData.clickedSegment ? updatedData.clickedSegment : null;
    updatedData = clickedSegment ? updatedData.data : updatedData;

    if (this.currentPitStop.label == this.CONVERSION_SCOPE) {
      // if (this.savedTechnologyScopePitData.conversion.length > 0) {
      //   for (let i of this.savedTechnologyScopePitData.conversion) {
      //     for (let j of i.children) {
      //       if (j.children.find(t => t.new == true)) {
      //         this.updateAllData(this.technologyScopePitOptionsData.conversion[0].dataTree, j);
      //       }
      //     }
      //   }
      // } else
      if (updatedData.length > 0) {
        for (let i of updatedData) {
          for (let j of i.children) {
            if (j.children.find(t => t.new == true)) {
              this.updateAllData(this.technologyScopePitOptionsData.conversion[0].dataTree, j);
            }
          }
        }
      }
      this.savedTechnologyScopePitData.conversion = updatedData;
    }
    else if (this.currentPitStop.label == this.REPORTS_SCOPE) {
      // if (this.savedTechnologyScopePitData.reports.length > 0) {
      //   for (let i of this.savedTechnologyScopePitData.reports) {
      //     for (let j of i.children) {
      //       if (j.children.find(t => t.new == true)) {
      //         this.updateAllData(this.technologyScopePitOptionsData.reports[0].dataTree, j);
      //       }
      //     }
      //   }
      // } else
      if (updatedData.length > 0) {
        for (let i of updatedData) {
          for (let j of i.children) {
            if (j.children.find(t => t.new == true)) {
              this.updateAllData(this.technologyScopePitOptionsData.reports[0].dataTree, j);
            }
          }
        }
      }
      this.savedTechnologyScopePitData.reports = updatedData;
    }
    else if (this.currentPitStop.label == this.INTERFACES_SCOPE) {
      // if (this.savedTechnologyScopePitData.interfaces.length > 0) {
      //   for (let i of this.savedTechnologyScopePitData.interfaces) {
      //     for (let j of i.children) {
      //       if (j.children.find(t => t.new == true)) {
      //         this.updateAllData(this.technologyScopePitOptionsData.interfaces[0].dataTree, j);
      //       }
      //     }
      //   }
      // } else
      if (updatedData.length > 0) {
        for (let i of updatedData) {
          for (let j of i.children) {
            if (j.children.find(t => t.new == true)) {
              this.updateAllData(this.technologyScopePitOptionsData.interfaces[0].dataTree, j);
            }
          }
        }
      }
      this.savedTechnologyScopePitData.interfaces = updatedData;
    }
    else if (this.currentPitStop.label == this.EXTENSION_SCOPE) {
      // if (this.savedTechnologyScopePitData.extensions.length > 0) {
      //   for (let i of this.savedTechnologyScopePitData.extensions) {
      //     for (let j of i.children) {
      //       if (j.children.find(t => t.new == true)) {
      //         this.updateAllData(this.technologyScopePitOptionsData.extensions[0].dataTree, j);
      //       }
      //     }
      //   }
      // } else
      if (updatedData.length > 0) {
        for (let i of updatedData) {
          for (let j of i.children) {
            if (j.children.find(t => t.new == true)) {
              this.updateAllData(this.technologyScopePitOptionsData.extensions[0].dataTree, j);
            }
          }
        }
      }
      this.savedTechnologyScopePitData.extensions = updatedData;
    }

    if(clickedSegment){
      this.next.emit({postData: this.savedTechnologyScopePitData, clickedSegment: clickedSegment});
    }

    else{
      if (indexOfCurrentPit == this.formSegment.length - 1) {
        this.next.emit(this.savedTechnologyScopePitData);
      } else {
        //this.subNext.emit();
        const nextSegment = this.formSegment[this.formSegment.indexOf(this.currentPitStop) + direction];
        this.next.emit({postData: this.savedTechnologyScopePitData, nextSegment: nextSegment});
        // this.activateFormSegment(nextSegment);      
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

  submit(event) {
    //this.postData.push({ projectType: [this.formData], geographicalScope: [], processScope: [], implementationApproach: [], phasePlanning: [] })
    this.next.emit(this.postData);
  }

}
