import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { NgForm } from '@angular/forms';
import { DateAdapter, MAT_DATE_FORMATS, MAT_DATE_LOCALE } from '@angular/material';
import { MAT_MOMENT_DATE_ADAPTER_OPTIONS, MAT_MOMENT_DATE_FORMATS, MomentDateAdapter } from '@angular/material-moment-adapter';
import { IMPLEMENTATION_APPROACH_OPTIONS } from '../../../constants/project-scope-generator/project-scope-stepper';


@Component({
  selector: 'app-implementation-approach-form',
  templateUrl: './implementation-approach-form.component.html',
  styleUrls: ['./implementation-approach-form.component.scss'],
  providers: [
    { provide: DateAdapter, useClass: MomentDateAdapter, deps: [MAT_DATE_LOCALE, MAT_MOMENT_DATE_ADAPTER_OPTIONS] },
    { provide: MAT_MOMENT_DATE_ADAPTER_OPTIONS, useValue: { useUtc: true } },
    { provide: MAT_DATE_FORMATS, useValue: MAT_MOMENT_DATE_FORMATS }
  ]
})
export class ImplementationApproachFormComponent implements OnInit {
  @ViewChild('implApproachForm', { static: false })
  ngForm: NgForm;
  @Input() formData: any;
  @Input() formOptions: any;
  @Input() projectId: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  postData: any[] = [];
  showError = false;

  readonly PHASED = IMPLEMENTATION_APPROACH_OPTIONS.PHASED;
  readonly BIG_BANG = IMPLEMENTATION_APPROACH_OPTIONS.BIG_BANG;

  formDataBkp: any;

  constructor() { }

  ngOnInit() {
    // if (this.formData.phaseDetails) {
    //   this.formDataBkp.phaseData = JSON.parse(JSON.stringify(this.formData))
    // }
  }

  ngOnChanges() {
    this.formData = this.formData ? this.formData : { implementationApproach: '' };
    this.formData.phaseCount = this.formData.phaseCount == null ? 0 : this.formData.phaseCount;
    if (this.formOptions && this.formData) {
      this.formData.phaseDetails = this.formData.phaseDetails ? this.formData.phaseDetails : [];
      this.formData.phaseCount = this.formData.phaseCount != 0 ? this.formData.phaseDetails.length : 1;
      this.formData.implementationApproach = this.formData.implementationApproach ? this.formData.implementationApproach : this.BIG_BANG;
      this.addPhase();
    }
    // this.formData.phaseDetails[0].phaseName = '';
  }

  isValid() {
    return (this.ngForm.valid)  //this.ngForm.valid &&
  }

  onNext(clickedSegment?:any) {
    event.preventDefault();
    this.showError = true;
    if (this.isValid()) { 
      if(clickedSegment){
        this.next.emit({postData: this.formData, clickedSegment: clickedSegment});
      }
      else     
        this.next.emit(this.formData);
      this.showError = false;
    }
    else{
      const firstElementWithError = document.querySelector('mat-form-field.ng-invalid');
      if(firstElementWithError){
        firstElementWithError.scrollIntoView({ behavior: 'smooth'});        
      }
    }
  }

  onPrev() {
    this.prev.emit();
  }

  addPhase() {

    let currLength = this.formData.phaseDetails.length;
    let count = this.formData.phaseCount;
    if (count > currLength) {
      for (let i = currLength; i < count; i++) {
        this.formData.phaseDetails.push({
          phaseName: '',
          startDate: '',
          endDate: ''
        });
      }
    }

    else if (count < currLength) {
      this.formData.phaseDetails.splice(count);
    }
  }

  changeView(expandElement, collapseElement) {
    expandElement.classList.toggle("visible");
    collapseElement.classList.toggle("visible");
  }

  removePhase(i: number) {
    this.formData.phaseDetails.splice(i, 1);
    this.formData.phaseCount--;
    this.formDataBkp = JSON.parse(JSON.stringify(this.formData))
  }

  radioBtnChanged(event) {
    // if (event.value.toUpperCase() == 'BIG BANG') {
    //   this.formDataBkp.phaseData = this.formPhasedBkpData();
    //   this.formData = this.formDataBkp.bigBangData;
    //   this.formDataBkp.bigBangData = this.formBigBangBkpData();
    // } else {
    //   this.formDataBkp.bigBangData = this.formBigBangBkpData();
    //   this.formData = this.formDataBkp.phaseData;
    //   this.formDataBkp.phaseData = this.formPhasedBkpData();
    // }
    // this.addPhase();

    let temp = this.formData;
    if (this.formDataBkp)
      this.formData = this.formBkpData(this.formData.implementationApproach);
    this.formDataBkp = JSON.parse(JSON.stringify({ phaseCount: temp.phaseCount, phaseDetails: temp.phaseDetails }));
    if (this.formData.implementationApproach == this.BIG_BANG)
      this.formData.phaseCount = 1;    
    this.addPhase();  
  }

  formBkpData(implementationApproach){
    return JSON.parse(JSON.stringify(
      {
        implementationApproach: implementationApproach,
        phaseCount: this.formDataBkp.phaseDetails.length,
        phaseDetails: this.formDataBkp.phaseDetails
      }
    ))
  }

  // formBigBangBkpData() {
  //   return JSON.parse(JSON.stringify(
  //     {
  //       implementationApproach: this.BIG_BANG,
  //       phaseCount: this.formData.phaseDetails.length,
  //       phaseDetails: this.formData.phaseDetails
  //     }
  //   ))
  // }

  // formPhasedBkpData() {
  //   return JSON.parse(JSON.stringify(
  //     {
  //       implementationApproach: this.PHASED,
  //       phaseCount: this.formData.phaseDetails.length,
  //       phaseDetails: this.formData.phaseDetails
  //     }
  //   ));
  // }
}
