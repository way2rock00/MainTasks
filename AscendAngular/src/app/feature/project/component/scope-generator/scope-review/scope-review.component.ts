import { nestedNode } from './../technology/technology.component';
import { Router } from '@angular/router';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { Subscription } from 'rxjs';
import { GeneratescopeService } from '../../../service/generatescope.service';
import { MatTreeNestedDataSource } from '@angular/material';
import { NestedTreeControl } from '@angular/cdk/tree';

@Component({
  selector: 'app-scope-review',
  templateUrl: './scope-review.component.html',
  styleUrls: ['./scope-review.component.scss']
})
export class ScopeReviewComponent implements OnInit {

  @Input() allPsgSavedData: any;
  @Output() prev = new EventEmitter();
  @Output() next = new EventEmitter();
  step = 0;

  projectTypeFilled: boolean = false;
  scopesFilled: boolean = false;
  implApprFilled: boolean = false;
  phasePlanningFilled: boolean = false;
  techScopeFilled: boolean = false;
  assumptionsFilled: boolean = false;

  treeControl = new NestedTreeControl<nestedNode>(node => node.children);
  geoScopeDataSource = new MatTreeNestedDataSource<nestedNode>();
  processScopeDataSource = new MatTreeNestedDataSource<nestedNode>();
  systemsDataSource = new MatTreeNestedDataSource<nestedNode>();
  conversionDataSource = new MatTreeNestedDataSource<nestedNode>();
  reportsDataSource = new MatTreeNestedDataSource<nestedNode>();
  interfacesDataSource = new MatTreeNestedDataSource<nestedNode>();
  extentionsDataSource = new MatTreeNestedDataSource<nestedNode>();
  assumptionsDataSource = new MatTreeNestedDataSource<nestedNode>();

  packageList: any[] = [];

  projectTypeNonMandatoryFields: any[] = ['SECONDARYMEMBERFIRM', 'SECONDARYPORTFOLIO', 'SECONDARYOFFERING', 'ADDITIONALERPPACKAGE', 'CLIENTGROUP']

  constructor(private generateScope: GeneratescopeService, private router: Router) {
  }

  ngOnInit() {

    if (this.allPsgSavedData.projectType) {
      if (this.allPsgSavedData.projectType[0]) {
        for (let i of Object.keys(this.allPsgSavedData.projectType[0])) {
          if (this.projectTypeNonMandatoryFields.indexOf(i.toUpperCase()) == -1 && (this.allPsgSavedData.projectType[0][i] || (this.allPsgSavedData.projectType[0][i] && this.allPsgSavedData.projectType[0][i].length && this.allPsgSavedData.projectType[0][i].length > 0))) {
            this.projectTypeFilled = true;
          } else {
            this.projectTypeFilled = false;
          }
        }
      } else {
        this.projectTypeFilled = true;
      }
    } else {
      this.projectTypeFilled = false;
    }

    if (this.allPsgSavedData.scopes[0].geographicalScope[0].regionCountry.length > 0 && this.allPsgSavedData.scopes[0].processScope[0].coreBusinessProcess.length > 0 && this.allPsgSavedData.scopes[0].systemScope.length > 0 && this.allPsgSavedData.scopes[0].serviceScope.length > 0) {
      this.scopesFilled = true;
    }

    if (this.allPsgSavedData.implementationApproach[0].phaseDetails.length > 0) {
      this.implApprFilled = true;
    }

    if (this.allPsgSavedData.phasePlanning != null) {
      this.phasePlanningFilled = true;
    }

    if (this.allPsgSavedData.technicalScope[0].conversion.length > 0 && this.allPsgSavedData.technicalScope[0].reports && this.allPsgSavedData.technicalScope[0].interfaces && this.allPsgSavedData.technicalScope[0].extensions.length > 0) {
      this.techScopeFilled = true;
    }

    if (this.allPsgSavedData.assumptions != null) {
      this.assumptionsFilled = true;
    }

    for (let savedPhaseObj of this.allPsgSavedData.phasePlanning)
      for (let tableObj of savedPhaseObj.tableDetails)
        for (let obj of tableObj.tableRowEntries) {
          for (let l1Obj of obj.l1Group)
            for (let packageItem of l1Obj.packageDetails) {
              if (this.packageList.indexOf(packageItem.packageName) == -1)
                this.packageList.push(packageItem.packageName)
            }
        }

    this.geoScopeDataSource.data = this.allPsgSavedData.scopes[0].geographicalScope[0].regionCountry;
    this.processScopeDataSource.data = this.allPsgSavedData.scopes[0].processScope[0].coreBusinessProcess;
    this.systemsDataSource.data = this.allPsgSavedData.scopes[0].systemScope;

    this.conversionDataSource.data = this.allPsgSavedData.technicalScope[0].conversion;
    this.reportsDataSource.data = this.allPsgSavedData.technicalScope[0].reports;
    this.interfacesDataSource.data = this.allPsgSavedData.technicalScope[0].interfaces;
    this.extentionsDataSource.data = this.allPsgSavedData.technicalScope[0].extensions;
    this.assumptionsDataSource.data = this.allPsgSavedData.assumptions;

  }

  changeView(expandElement, collapseElement) {
    expandElement.classList.toggle("visible");
    collapseElement.classList.toggle("visible");
  }

  hasChild = (_: number, node: nestedNode) => !!node.children && node.children.length > 0;

  setStep(index: number) {
    this.step = index;
  }

  goBack() {
    this.router.navigate(['/project/list'])
  }

  goto(route) {
    window.open('project/psg/' + this.router.url.split('/')[3] + '/' + route, "_self")
    // this.router.navigate(['project/psg/' + this.router.url.split('/')[3] + '/' + route])
  }

  nextStep() {
    this.step++;
  }

  prevStep() {
    this.step--;
  }

  onPrev() {
    this.prev.emit();
  }

  onNext(clickedSegment: any){
    this.next.emit({clickedSegment: clickedSegment});
  }
}
