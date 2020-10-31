import { Component, OnInit, Output, EventEmitter, Input } from '@angular/core';
import { MAT_DATE_LOCALE, DateAdapter, MAT_DATE_FORMATS } from '@angular/material';
import { MomentDateAdapter, MAT_MOMENT_DATE_ADAPTER_OPTIONS, MAT_MOMENT_DATE_FORMATS } from '@angular/material-moment-adapter';
import * as _moment from 'moment';

@Component({
  selector: 'app-phase-planning-form',
  templateUrl: './phase-planning-form.component.html',
  styleUrls: ['./phase-planning-form.component.scss'],
  providers: [
    { provide: DateAdapter, useClass: MomentDateAdapter, deps: [MAT_DATE_LOCALE, MAT_MOMENT_DATE_ADAPTER_OPTIONS] },
    { provide: MAT_MOMENT_DATE_ADAPTER_OPTIONS, useValue: { useUtc: true } },
    { provide: MAT_DATE_FORMATS, useValue: MAT_MOMENT_DATE_FORMATS }
  ]
})
export class PhasePlanningFormComponent implements OnInit {

  @Input() phasePlanning: any;
  @Input() savedPhasePlanning: any;

  filterProcesses: any = [];

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  postData: any[] = [];
  currPhase: string;

  packageList: any[] = [];
  countryMasterList = [];
  moduleList = new Map<any, any>();

  deleteErrorMsg: boolean = false;

  constructor() { }

  ngOnInit() {
    console.log('Printing Inputs');
    console.log(this.phasePlanning);
    console.log(this.savedPhasePlanning);
  }

  ngOnChanges() {
    if (this.savedPhasePlanning)
      this.currPhase = this.savedPhasePlanning[0].phaseName;
    if (this.phasePlanning && this.savedPhasePlanning)
      this.initializeData();
  }

  onNext(clickedSegment?: any) {
    event.preventDefault();
    // if (this.isValid()) {
    //this.postData.push({ projectType: [], geographicalScope: [], processScope: [], implementationApproach: [], phasePlanning: this.savedPhasePlanning })
    if(clickedSegment)
      this.next.emit({postData: this.savedPhasePlanning, clickedSegment: clickedSegment});
    else
      this.next.emit(this.savedPhasePlanning);
    // }
  }

  onPrev() {
    this.prev.emit();
  }

  result = {};

initializeData() {

  this.countryMasterList = this.savedPhasePlanning[0].countries;
  
  //Intial moduleList map for all combinations of L1, L2, L3, package infor
  for (let obj of this.phasePlanning.tableEntries) {
    for (let l1Obj of obj.l1Group) {
      for (let packageItem of l1Obj.packageDetails) {
        this.moduleList.set(JSON.stringify({
          l1: obj.l1Value,
          l2: l1Obj.l2Value,
          l3: l1Obj.l3Value,
          l4: packageItem.packageName
        }
        ), packageItem.modules)
      }
    }
  }

  //Update saved info with all modules info
  for (let savedPhaseObj of this.savedPhasePlanning)
    for (let tableObj of savedPhaseObj.tableDetails)
      for (let obj of tableObj.tableRowEntries) {
        if (this.filterProcesses.indexOf(obj.l1Value) == -1)
          this.filterProcesses.push(obj.l1Value)
        for (let l1Obj of obj.l1Group)
          for (let packageItem of l1Obj.packageDetails) {
            if (this.packageList.indexOf(packageItem.packageName) == -1)
              this.packageList.push(packageItem.packageName)

            packageItem.allModules = this.moduleList.get(JSON.stringify(
              {
                l1: obj.l1Value,
                l2: l1Obj.l2Value,
                l3: l1Obj.l3Value,
                l4: packageItem.packageName
              }))
          }
      }
}

addTable(phaseIndex, tableIndex) {
  let prevTable = this.savedPhasePlanning[phaseIndex].tableDetails[tableIndex];
  let rowEntries = JSON.parse(JSON.stringify(prevTable.tableRowEntries));
  for (let rowDet of rowEntries)
    for (let level of rowDet.l1Group)
      for (let packageDet of level.packageDetails) {
        packageDet.modules = [];
      }
  let newTable = { tablecountries: [], tableRowEntries: rowEntries, id: this.savedPhasePlanning[phaseIndex].tableDetails.length }

  this.savedPhasePlanning[phaseIndex].tableDetails.splice(tableIndex + 1, 0, newTable)
}


removeTable(phaseIndex, tableIndex) {

  if (this.savedPhasePlanning[phaseIndex].tableDetails.length > 1) {
    this.deleteErrorMsg = false
    this.savedPhasePlanning[phaseIndex].tableDetails.splice(tableIndex, 1);
  }
  else
    this.deleteErrorMsg = true;

}

changeView(expandElement, collapseElement) {
  expandElement.classList.toggle("visible");
  collapseElement.classList.toggle("visible");
}
}
