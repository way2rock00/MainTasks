import { Component, ViewEncapsulation, Input, Output, EventEmitter, ViewChild } from '@angular/core';
import { CreateprojectService } from 'src/app/feature/project/service/createproject.service';
import { ScopeDetailsModel } from 'src/app/feature/project/model/add-edit-project/scope-details.model';
import { NgForm } from '@angular/forms';

@Component({
    selector: 'app-scope-details-form',
    styleUrls: ['./scope-details.component.scss'],
    templateUrl: './scope-details.component.html',
    encapsulation: ViewEncapsulation.None
})
export class ScopeDetailsForm {
    @Input()
    formData: ScopeDetailsModel;

    formOptions: any;

    @Output()
    onSubmitForm: EventEmitter<any> = new EventEmitter<any>();

    @Output()
    back: EventEmitter<any> = new EventEmitter<any>();        

    @ViewChild('scopeForm', {static: false})
    ngForm: NgForm;

    isSubmitted: boolean= false;

    constructor(private projectService: CreateprojectService) {}

    ngOnInit() {
        this.projectService.fetchScopeDetails()
        .subscribe((data) => {
            this.formOptions = data;
            if (this.formData.businessProcess) {
                this.setBusinessProcessL2List(this.formData.businessProcess)
            }
        })
    }

    setProcessL2ForProcessL1(businessProcessl1Name) {
            for(let businessprocess of this.formOptions.businessprocessL1L2) {
                if (businessprocess.processareal1 === businessProcessl1Name) {
                    let businessProcessl2Names = businessprocess.processareal2||[];
                    for (let l2Index = 0; l2Index < businessProcessl2Names.length; l2Index++) {
                        let index = this.formData.businessProcessl2.indexOf(businessProcessl2Names[l2Index]);
                        if(index==-1)
                        this.formData.businessProcessl2.push(businessProcessl2Names[l2Index]);
                    }
                }
            }

    }   

    setBusinessProcessL2List(value) {
        this.formOptions.businessProcessl2 = [];
        for (let index = 0; index < value.length; index++) {
            let businessProcessl1Name = value[index];
            for(let businessprocess of this.formOptions.businessprocessL1L2) {
                if (businessprocess.processareal1 === businessProcessl1Name) {
                    let businessProcessl2Names = businessprocess.processareal2||[];
                    for (let l2Index = 0; l2Index < businessProcessl2Names.length; l2Index++) {
                        this.formOptions.businessProcessl2.push(businessProcessl2Names[l2Index]);
                    }
                }
            }

        }
    }   
    
    getBusinessProcessL1State(value){
        if(this.formData.businessProcess.indexOf(value)>-1)
        return true;
        else return false;
    }

    getBusinessProcessL2State(value){
        if(this.formData.businessProcessl2.indexOf(value)>-1)
        return true;
        else return false;
    }  
    
    businessProcessL1Changed(checked,value){
        if(checked){
            let index = this.formData.businessProcess.indexOf(value);
            if(index==-1){
                this.formData.businessProcess.push(value);
                //For newly selected industry select all the related sectors as well.
                this.setProcessL2ForProcessL1(value);                
            }
        }
        else{
            // console.log(value);
            let  businessprocess = this.formOptions.businessprocessL1L2;
            for (let index = 0; index < businessprocess.length; index++) {
                if(businessprocess[index].processareal1 == value){
                    let businessProcessl2Names = businessprocess[index].processareal2;
                    for (let l2Index = 0; l2Index < businessProcessl2Names.length; l2Index++) {
                        let businessProcessl2Name = businessProcessl2Names[l2Index];
                        let businessProcessl2NameIndex = this.formData.businessProcessl2.indexOf(businessProcessl2Name);
                        if(businessProcessl2NameIndex!=-1){
                            this.formData.businessProcessl2.splice(businessProcessl2NameIndex,1);
                        }
                    }
                }

            }
            this.formData.businessProcess.splice(this.formData.businessProcess.indexOf(value),1);
        }
        this.setBusinessProcessL2List(this.formData.businessProcess);
    }    

    businessProcessL2Changed(checked,value){
        if(checked){
            let index = this.formData.businessProcessl2.indexOf(value);
            if(index==-1)
            this.formData.businessProcessl2.push(value);
        }
        else
            this.formData.businessProcessl2.splice(this.formData.businessProcessl2.indexOf(value),1);
    }    



    onSubmit(clientForm: NgForm) {
        event.preventDefault();
        if (this.isValid()) {
            this.isSubmitted = true;
            this.onSubmitForm.emit(this.formData);
        }
    }

    goBack(){
        this.back.emit();
    }   
    
    isValid(){
        return (this.formData.businessProcess.length && this.formData.businessProcessl2.length && this.ngForm.valid && this.businessProcessL1L2Combination())
    }

    businessProcessL1L2Combination(){
        for (let businessProcessIndex = 0; businessProcessIndex < this.formData.businessProcess.length; businessProcessIndex++) {
            if(this.formData.businessProcess[businessProcessIndex] != ''){
            let businessProcessL1Name = this.formData.businessProcess[businessProcessIndex];
            let counter: number = 0;
            for (let completeBusinessProcessIndex = 0; completeBusinessProcessIndex < this.formOptions.businessprocessL1L2.length; completeBusinessProcessIndex++) {
                if(this.formOptions.businessprocessL1L2[completeBusinessProcessIndex].processareal1 == businessProcessL1Name){
                    for (let businessProcessL2Index = 0; businessProcessL2Index < this.formOptions.businessprocessL1L2[completeBusinessProcessIndex].processareal2.length; businessProcessL2Index++) {
                        const element = this.formOptions.businessprocessL1L2[completeBusinessProcessIndex].processareal2[businessProcessL2Index];
                        if(this.formData.businessProcessl2.indexOf(element)>-1)   
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