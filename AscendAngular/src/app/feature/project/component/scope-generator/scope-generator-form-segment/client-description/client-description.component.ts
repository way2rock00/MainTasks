import { Component, EventEmitter, Input, OnInit, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { NgForm } from '@angular/forms';
import { ScopeGeneratorFormModel } from 'src/app/feature/project/model/project-scope-generator/scope-generator-form.model';
import { GeneratescopeService } from 'src/app/feature/project/service/generatescope.service';

@Component({
  selector: 'app-client-description',
  templateUrl: './client-description.component.html',
  styleUrls: ['./client-description.component.scss'],
  encapsulation: ViewEncapsulation.None,
})
export class ClientDescriptionForm implements OnInit {

  @Input()
  formData: ScopeGeneratorFormModel

  @Input() formOptions: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  @Input()
  isEditable: boolean;

  @ViewChild('clientForm', { static: false })
  ngForm: NgForm;

  showError: boolean = false;

  constructor(private projectService: GeneratescopeService) { }

  ngOnInit() {
    if (this.formData.industry) {
      //this.onIndustryChanged(this.formData.industry, true)
      this.setSectorList(this.formData.industry)
    }
  }

  setSectorList(value) {
    this.formOptions.sector = [];
    // for (let index = 0; index < value.length; index++) {
    //let industryName = value[index];
    for (let industries of this.formOptions.industrySector) {
      if (industries.industry === value) {
        let sectorNames = industries.sector || [];
        for (let sectorIndex = 0; sectorIndex < sectorNames.length; sectorIndex++) {
          this.formOptions.sector.push(sectorNames[sectorIndex]);

        }
      }
    }
  }

  submit(form){
    console.log(form);    
  }

  onNext(clickedSegment?:any) {
    console.log('Client tab:nextClicked');
    event.preventDefault();
    this.ngForm.ngSubmit.emit(true);
    this.showError = true;
    if (this.isValid()) {
    // this.next.emit(this.formData);
    this.showError = false;
    if(clickedSegment)
      this.next.emit({data:this.formData, clickedSegment: clickedSegment});
    else
      this.next.emit(this.formData);
    }
    else{
      const firstElementWithError = document.querySelector('.ng-invalid,.multi-select-error-div');
      if(firstElementWithError)
        firstElementWithError.scrollIntoView({ behavior: 'smooth' });
    }
  }

  onPrev() {
    console.log('Client tab:prevClicked');
    this.prev.emit();
  }

  getIndustriesState(value) {
    if (this.formData.industry.indexOf(value) > -1)
      return true;
    else return false;
  }

  getSectorState(value) {
    if (this.formData.sector.indexOf(value) > -1)
      return true;
    else return false;
  }

  industriesChanged(checked, value) {
    if (checked) {
      let index = this.formData.industry.indexOf(value);
      if (index == -1) {
        //this.formData.industry.push(value);
        this.formData.industry = value;
        this.formData.sector = "";
        //For newly selected industry select all the related sectors as well.
        //this.setSectorsForIndustry(value);
      }
    }
    else {
      // console.log(value);
      let industries = this.formOptions.industrySector;
      for (let index = 0; index < industries.length; index++) {
        // console.log('Test3:'+industries[index].industry);
        if (industries[index].industry == value) {
          let sectorNames = industries[index].sector;
          // console.log('Test1.1:'+sectorNames);
          for (let secIndex = 0; secIndex < sectorNames.length; secIndex++) {
            let sectorName = sectorNames[secIndex];
            let sectorIndex = this.formData.sector.indexOf(sectorName);
            // console.log('Test2:'+'Index:'+sectorIndex+':'+sectorName);
            if (sectorIndex != -1) {
              //this.formData.sector.splice(sectorIndex, 1);
              this.formData.sector = "";
            }
          }
        }

      }
      //this.formData.industry.splice(this.formData.industry.indexOf(value), 1);
      this.formData.industry = "";
    }
    this.setSectorList(this.formData.industry);
  }

  sectorChange(checked, value) {
    if (checked) {
      let index = this.formData.sector.indexOf(value);
      if (index == -1)
        //this.formData.sector.push(value);
        this.formData.sector = value;
    }
    else
      //this.formData.sector.splice(this.formData.sector.indexOf(value), 1);
      this.formData.sector = "";
    // console.log(this.formData.sector);
  }

  isValid() {
    return (this.ngForm.valid && this.formData.industry.length && this.formData.sector.length && this.industrySectorCombination())  //this.ngForm.valid &&
  }

  industrySectorCombination() {
    //console.log(this.formData.industry);
    //console.log(this.formData.sector);
    //console.log(this.formOptions.portfolioOfferings);
    if (this.formData.industry != '') {
      let industry = this.formData.industry;
      let counter: number = 0;
      for (let completeIndustryIndex = 0; completeIndustryIndex < this.formOptions.industrySector.length; completeIndustryIndex++) {
        if (this.formOptions.industrySector[completeIndustryIndex].industry == industry) {
          for (let sectorIndex = 0; sectorIndex < this.formOptions.industrySector[completeIndustryIndex].sector.length; sectorIndex++) {
            const element = this.formOptions.industrySector[completeIndustryIndex].sector[sectorIndex];
            if (this.formData.sector.indexOf(element) > -1)
              counter = counter + 1
          }
        }
      }
      // console.log('offeringPortfolio:'+offeringPortfolio+':'+counter);
      if (counter == 0)
        return false;
    }
    //}
    return true;
  }

}
