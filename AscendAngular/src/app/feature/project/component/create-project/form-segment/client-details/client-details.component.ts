import { Component, ViewEncapsulation, Input, Output, EventEmitter, ViewChild } from '@angular/core';
import { CreateprojectService } from 'src/app/feature/project/service/createproject.service';
import { ClientDetailsModel } from 'src/app/feature/project/model/add-edit-project/client-details.model';
import { NgForm } from '@angular/forms';

@Component({
    selector: 'app-client-details-form',
    styleUrls: ['./client-details.component.scss'],
    templateUrl: './client-details.component.html',
    encapsulation: ViewEncapsulation.None
})
export class ClientDetailsForm {
    @Input()
    formData: ClientDetailsModel

    formOptions: any;

    @Output()
    next: EventEmitter<any> = new EventEmitter<any>();

    @Output()
    back: EventEmitter<any> = new EventEmitter<any>();

    @Input()
    isEditable: boolean;

    @ViewChild('clientForm', {static: false})
    ngForm: NgForm;

    constructor(private projectService: CreateprojectService) {}

    ngOnInit() {
        this.projectService.fetchClientDetails()
        .subscribe((data) => {
            this.formOptions = data;
            if (this.formData.industry) {
                //this.onIndustryChanged(this.formData.industry, true)
                this.setSectorList(this.formData.industry)
            }
        })
    }

    /*onIndustryChanged(industryName){
        //If Industry has been unchecked then uncheck values in sector for that unchecked industry.
        // console.log('Test1:'+this.formData.sector+':'+industryName);
        if(this.formData.industry.indexOf(industryName) == -1){
            let  industries = this.formOptions.industrySector;
            for (let index = 0; index < industries.length; index++) {
                // console.log('Test3:'+industries[index].industry);
                if(industries[index].industry == industryName){
                    let sectorNames = industries[index].sector;
                    // console.log('Test1.1:'+sectorNames);
                    for (let secIndex = 0; secIndex < sectorNames.length; secIndex++) {
                        let sectorName = sectorNames[secIndex];
                        let sectorIndex = this.formData.sector.indexOf(sectorName);
                        // console.log('Test2:'+'Index:'+sectorIndex+':'+sectorName);
                        if(sectorIndex!=-1){
                            this.formData.sector.splice(sectorIndex,1);
                        }
                    }
                }

            }
        }
        this.setSectorList(this.formData.industry);
    }*/
    setSectorList(value) {
        this.formOptions.sector = [];
        for (let index = 0; index < value.length; index++) {
            let industryName = value[index];
            for(let industries of this.formOptions.industrySector) {
                if (industries.industry === industryName) {
                    let sectorNames = industries.sector||[];
                    for (let sectorIndex = 0; sectorIndex < sectorNames.length; sectorIndex++) {
                        this.formOptions.sector.push(sectorNames[sectorIndex]);

                    }
                }
            }

        }
        /*
        this.formOptions.sector = [];
        if (!skipSectorset) {
            this.formData.sector = [];
        }

        if (this.formOptions
            && this.formOptions.industrySector
            && this.formOptions.industrySector.length
        ) {
            for(let industrySector of this.formOptions.industrySector) {
                if (industrySector.industry === value) {
                    this.formOptions.sector = industrySector.sector || [];
                    if (!skipSectorset && this.formOptions.sector.length === 1) {
                        this.formData.sector = this.formOptions.sector[0];
                    }
                    break;
                }
            }
        }*/
    }


    /* Commented select All logic for Offering and Portfolio
    setSectorsForIndustry(industryName) {
            for(let industries of this.formOptions.industrySector) {
                if (industries.industry === industryName) {
                    let sectorNames = industries.sector||[];
                    for (let sectorIndex = 0; sectorIndex < sectorNames.length; sectorIndex++) {
                        let index = this.formData.sector.indexOf(sectorNames[sectorIndex]);
                        if(index==-1)
                        this.formData.sector.push(sectorNames[sectorIndex]);                        
                    }
                }
            }

    }*/

    onNext(clientForm: NgForm) {
        event.preventDefault();
        if (this.isValid()) {
            this.next.emit(this.formData);
        }
    }

    goBack(){
        this.back.emit();
    }

    getIndustriesState(value){
        if(this.formData.industry.indexOf(value)>-1)
        return true;
        else return false;
    }

    getSectorState(value){
        if(this.formData.sector.indexOf(value)>-1)
        return true;
        else return false;
    }

    industriesChanged(checked,value){
        if(checked){
            let index = this.formData.industry.indexOf(value);
            if(index==-1){
                this.formData.industry.push(value);
                //For newly selected industry select all the related sectors as well.
                //this.setSectorsForIndustry(value);
            }
        }
        else{
            // console.log(value);
            let  industries = this.formOptions.industrySector;
            for (let index = 0; index < industries.length; index++) {
                // console.log('Test3:'+industries[index].industry);
                if(industries[index].industry == value){
                    let sectorNames = industries[index].sector;
                    // console.log('Test1.1:'+sectorNames);
                    for (let secIndex = 0; secIndex < sectorNames.length; secIndex++) {
                        let sectorName = sectorNames[secIndex];
                        let sectorIndex = this.formData.sector.indexOf(sectorName);
                        // console.log('Test2:'+'Index:'+sectorIndex+':'+sectorName);
                        if(sectorIndex!=-1){
                            this.formData.sector.splice(sectorIndex,1);
                        }
                    }
                }

            }
            this.formData.industry.splice(this.formData.industry.indexOf(value),1);
        }
        this.setSectorList(this.formData.industry);
    }

    sectorChange(checked,value){
        if(checked){
            let index = this.formData.sector.indexOf(value);
            if(index==-1)
            this.formData.sector.push(value);
        }
        else
            this.formData.sector.splice(this.formData.sector.indexOf(value),1);
        // console.log(this.formData.sector);
    }

    isValid(){
        return (this.formData.industry.length && this.formData.sector.length && this.ngForm.valid && this.industrySectorCombination())
    }

    industrySectorCombination(){
        //console.log(this.formData.industry);
        //console.log(this.formData.sector);
        //console.log(this.formOptions.portfolioOfferings);
        for (let industryIndex = 0; industryIndex < this.formData.industry.length; industryIndex++) {
            if(this.formData.industry[industryIndex] != ''){
            let industry = this.formData.industry[industryIndex];
            let counter: number = 0;
            for (let completeIndustryIndex = 0; completeIndustryIndex < this.formOptions.industrySector.length; completeIndustryIndex++) {
                if(this.formOptions.industrySector[completeIndustryIndex].industry == industry){
                    for (let sectorIndex = 0; sectorIndex < this.formOptions.industrySector[completeIndustryIndex].sector.length; sectorIndex++) {
                        const element = this.formOptions.industrySector[completeIndustryIndex].sector[sectorIndex];
                        if(this.formData.sector.indexOf(element)>-1)   
                        counter = counter + 1                     
                    }
                }
            }
           // console.log('offeringPortfolio:'+offeringPortfolio+':'+counter);
            if(counter==0)
            return false;
        }
        }
        return true;
    }


}
