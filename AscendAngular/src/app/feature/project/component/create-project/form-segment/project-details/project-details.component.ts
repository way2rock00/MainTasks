import { Component, ViewEncapsulation, OnInit, Output, EventEmitter, Input, ViewChild } from '@angular/core';
import { CreateprojectService } from 'src/app/feature/project/service/createproject.service';
import { ProjectDetailsModel } from 'src/app/feature/project/model/add-edit-project/project-details.model';
import { NgForm, FormControl, Validators } from '@angular/forms';
import { debounceTime, tap, switchMap, finalize, startWith, map } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { HttpServiceHelper } from '../../../../../../types/common/HttpServiceHelper';

import { HttpParams } from '@angular/common/http';
import { User } from 'src/app/feature/project/constants/ascend-user-info';

const imageType: string[] = ['TIFF', 'PJP', 'PJPEG', 'JFIF', 'WEBP', 'TIF', 'BMP', 'PNG', 'JPEG', 'SVG', 'JPG', 'GIF', 'SVGZ', 'ICO', 'XMB', 'DIB'];
const documentType: string[] = ['XLS', 'XLSX', 'PDF', 'DOC', 'DOCX'];

@Component({
    selector: 'app-project-details-form',
    styleUrls: ['./project-details.component.scss'],
    templateUrl: './project-details.component.html',
    encapsulation: ViewEncapsulation.None
})
export class ProjectDetailsForm implements OnInit {

    @Input()
    formData: ProjectDetailsModel;

    formOptions: any;

    @Output()
    next: EventEmitter<any> = new EventEmitter<any>();

    @Output()
    back: EventEmitter<any> = new EventEmitter<any>();

    @Input()
    isEditable: boolean;

    @ViewChild('projectForm', { static: false })
    ngForm: NgForm;

    constructor(private projectService: CreateprojectService
        , private httpService: HttpServiceHelper
    ) { }

    typeOfProject: string[] = ['Implementation', 'Operate', 'Upgrade'];

    searchManager = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
    searchLeadPD = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
    isLoading = false;
    errorMsg: string;
    url = 'https://graph.microsoft.com/v1.0/users';
    filteredUser: any = [];
    filteredPDUser: any = [];


    ngOnInit() {
        this.projectService.fetchProjectDetails()
            .subscribe((data) => {
                this.formOptions = data[0];
                if (this.formData.offeringPortfolio) {
                    this.setOfferingList(this.formData.offeringPortfolio)
                }
                if (this.formData.industry) {
                    this.setSectorList(this.formData.industry);
                }
                this.searchManager.setValue(this.formData.projectManager);
                this.searchLeadPD.setValue(this.formData.leadPD)
            })


        this.searchManager.valueChanges
            .pipe(
                debounceTime(500),
                tap(() => {
                    this.errorMsg = '';
                    // this.filteredUser = [];
                    this.isLoading = true;
                }),
                // tslint:disable-next-line: max-line-length
                switchMap(value => {
                    if (value !== '') {
                        //console.log('inside if');
                        return this.httpService.httpGetRequestWithParams(this.url, new HttpParams().set('$filter', "((startswith(displayName,'" + value + "') or startswith(mail,'" + value + "')) and userType eq 'Member')"))
                            .pipe(
                                finalize(() => {
                                    this.isLoading = false;
                                }),
                            )
                    } else {
                        //console.log('inside else');
                        // if no value is present, return null
                        return Observable.of(false);
                    }
                }
                )
            )
            .subscribe((datas: any) => {
                console.log('inside subscribe');
                console.log(datas)
                //const usersList: User[] = [];
                this.filteredUser = [];
                if (datas) {
                    let obj = datas;
                    // tslint:disable-next-line: prefer-for-of
                    for (let index = 0; index < obj.value.length; index++) {
                        const user: User = new User();
                        let email = obj.value[index].mail;
                        console.log(' Email:' + email);
                        if (email && (email.includes('deloitte.') || email.includes('DELOITTE.'))) {
                            user.userId = obj.value[index].mail;
                            user.userName = obj.value[index].displayName;
                            user.ssoUser.displayName = obj.value[index].displayName;
                            user.ssoUser.givenName = obj.value[index].givenName;
                            user.ssoUser.surname = obj.value[index].surname;
                            user.ssoUser.jobTitle = obj.value[index].jobTitle;

                            console.log('UserId' + user.userId + 'UserName:' + user.userName);
                            this.filteredUser.push(user);
                        }
                        //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/
                        if (this.filteredUser.length == 1) {
                            this.setSelectedManager(user);
                        }
                    }
                } else {
                    this.formData.projectManager = '';
                    this.formData.mgrfirstname = '';
                    this.formData.mgrlastname = '';
                    this.formData.mgrusername = '';
                    this.formData.mgrjobtitle = '';
                }

            });

        this.searchLeadPD.valueChanges
            .pipe(
                debounceTime(500),
                tap(() => {
                    this.errorMsg = '';
                    // this.filteredUser = [];
                    this.isLoading = true;
                }),
                // tslint:disable-next-line: max-line-length
                switchMap(value => {
                    if (value !== '') {

                        //console.log('inside if');
                        return this.httpService.httpGetRequestWithParams(this.url, new HttpParams().set('$filter', "((startswith(displayName,'" + value + "') or startswith(mail,'" + value + "')) and userType eq 'Member')"))
                            .pipe(
                                finalize(() => {
                                    this.isLoading = false;
                                }),
                            )
                    } else {
                        //console.log('inside else');
                        // if no value is present, return null
                        return Observable.of(false);
                    }
                }
                )
            )
            .subscribe((datas: any) => {
                console.log('inside subscribe');
                console.log(datas)
                //const usersList: User[] = [];
                this.filteredPDUser = [];
                if (datas) {
                    let obj = datas;
                    // tslint:disable-next-line: prefer-for-of
                    for (let index = 0; index < obj.value.length; index++) {
                        const user: User = new User();
                        let email = obj.value[index].mail;
                        console.log(' Email:' + email);
                        if (email && (email.includes('deloitte.') || email.includes('DELOITTE.'))) {
                            user.userId = obj.value[index].mail;
                            user.userName = obj.value[index].displayName;
                            user.ssoUser.displayName = obj.value[index].displayName;
                            user.ssoUser.givenName = obj.value[index].givenName;
                            user.ssoUser.surname = obj.value[index].surname;
                            user.ssoUser.jobTitle = obj.value[index].jobTitle;
                            console.log('UserId' + user.userId + 'UserName:' + user.userName);
                            this.filteredPDUser.push(user);
                        }
                        //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/
                        if (this.filteredPDUser.length == 1) {
                            this.setSelectedLeadPD(user);
                        }
                    }
                } else {
                    this.formData.leadPD = '';
                    this.formData.ppdfirstname = '';
                    this.formData.ppdlastname = '';
                    this.formData.ppdusername = '';
                    this.formData.ppdjobtitle = '';
                }

            });
    }


    setSelectedManager(user) {
        this.formData.projectManager = user.userId;
        this.formData.mgrfirstname = user.ssoUser.givenName;
        this.formData.mgrlastname = user.ssoUser.surname;
        this.formData.mgrusername = user.ssoUser.displayName;
        this.formData.mgrjobtitle = user.ssoUser.jobTitle;
    }

    setSelectedLeadPD(user) {
        this.formData.leadPD = user.userId;
        this.formData.ppdfirstname = user.ssoUser.givenName;
        this.formData.ppdlastname = user.ssoUser.surname;
        this.formData.ppdusername = user.ssoUser.displayName;
        this.formData.ppdjobtitle = user.ssoUser.jobTitle;
    }
    /*
    onPortfolioChanged(offeringName){
        //If Offering Portfolio has been unchecked then uncheck values in offering for that unchecked portfolio.
        if(this.formData.offeringPortfolio.indexOf(offeringName) == -1){
            let  portfolioOfferings = this.formOptions.portfolioOfferings;
            for (let index = 0; index < portfolioOfferings.length; index++) {
                if(portfolioOfferings[index].portfolioName == offeringName){
                    let offeringNames = portfolioOfferings[index].offeringName;
                    for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
                        let offeringName = offeringNames[offIndex];
                        let offeringIndex = this.formData.offering.indexOf(offeringName);
                        if(offeringIndex!=-1){
                            this.formData.offering.splice(offeringIndex,1);
                        }
                    }
                }

            }
        }
        this.setOfferingList(this.formData.offeringPortfolio);
    }*/

    /* Commented select All logic for Offering and Portfolio

    setOfferingForPortfolio(portfolioName) {
            for(let portfolio of this.formOptions.portfolioOfferings) {
                if (portfolio.portfolioName === portfolioName) {
                    let offeringNames = portfolio.offeringName||[];
                    for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
                        let index = this.formData.offering.indexOf(offeringNames[offIndex]);
                        if(index==-1)
                        this.formData.offering.push(offeringNames[offIndex]);
                    }
                }
            }
    }*/

    setOfferingList(value) {
        this.formOptions.offerings = [];
        //for (let index = 0; index < value.length; index++) {
            //let portfolioName = value[index];
           // console.log(this.formOptions);

            for (let portfolio of this.formOptions.portfolioOfferings) {
                if (portfolio.portfolioName === value) {
                    let offeringNames = portfolio.offeringName || [];
                    for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
                        this.formOptions.offerings.push(offeringNames[offIndex]);

                    }
                }
            }

       // }

      
        /*
        this.formOptions.offerings = [];
        if (!skipOfferingSet) {
            this.formData.offering = [];
        }

        if (this.formOptions
            && this.formOptions.portfolioOfferings
            && this.formOptions.portfolioOfferings.length
        ) {
            for(let portfolio of this.formOptions.portfolioOfferings) {
                if (portfolio.portfolioName === value) {
                    this.formOptions.offerings = portfolio.offeringName || [];
                    if (!skipOfferingSet && this.formOptions.offerings.length === 1) {
                        this.formData.offering = this.formOptions.offerings[0];
                    }
                    break;
                }
            }
        }*/
    }

    erpChecked(event, value) {
        // console.log(event);
        if (event.checked) {
            this.formData.erpPackage.push(value);
        } else {
            this.formData.erpPackage = this.formData.erpPackage.filter(pack => pack !== value)
        }
    }

    uploadFile(event) {
        let file = event.target.files[0];
        if (file) {
            let fileExtension = file.name.substring(file.name.indexOf('.') + 1).toUpperCase();
            if (imageType.indexOf(fileExtension) > -1)
                this.formData.logoFile = event.target.files[0];
            else
                alert('Only Image files are supported');
        }
    }

    uploadDocuments(event) {
        let file = event.target.files[0];
        if (file) {
            let fileExtension = file.name.substring(file.name.indexOf('.') + 1).toUpperCase();
            if (documentType.indexOf(fileExtension) > -1)
                this.formData.documents = event.target.files[0];
            else
                alert('Only Excel, Document and PDF files are supported');
        }
    }

    onNext(projectForm: NgForm) {
        // console.log('Submit Event fired');
        // console.log(this.formData);

        event.preventDefault();
        if (this.isValid()) {
            this.next.emit(this.formData);
        }
    }

    goBack() {
        this.back.emit();
    }

    getPortfolioOfferingsState(value) {
        // if (this.formData.offeringPortfolio.indexOf(value) > -1)
        //     return true;
        // else return false;
       return this.formData.offeringPortfolio == value;
    }

    getOfferingState(value) {
        // if (this.formData.offering.indexOf(value) > -1)
        //     return true;
        // else return false;
       return this.formData.offering == value;
    }

    portfolioOfferingsChanged(checked, value) {
        if (checked) {
            this.formData.offeringPortfolio = value;
            this.formData.offering = "";
            //}
        }
        else {

            this.formData.offering = "";
            this.formData.offeringPortfolio = "";

        }
        this.setOfferingList(this.formData.offeringPortfolio);
    }

    offeringChange(checked, value) {
        if (checked) {
            this.formData.offering = value;
        }
        else

            this.formData.offering = "";
    }

    isValid() {
        return (this.formData.offeringPortfolio.length && this.formData.offering.length && !this.searchManager.invalid && !this.searchLeadPD.invalid
            && this.ngForm.valid && this.offeringPortfolioCombination() && this.formData.industry.length &&
            this.formData.sector.length && this.ngForm.valid && this.industrySectorCombination())
    }

    offeringPortfolioCombination() {
        //console.log(this.formData.offeringPortfolio);
        //console.log(this.formData.offering);
        //console.log(this.formOptions.portfolioOfferings);
       // for (let portfolioIndex = 0; portfolioIndex < this.formData.offeringPortfolio.length; portfolioIndex++) {
            if (this.formData.offeringPortfolio != '') {
                let offeringPortfolio = this.formData.offeringPortfolio;
                let counter: number = 0;
                for (let completeOffIndex = 0; completeOffIndex < this.formOptions.portfolioOfferings.length; completeOffIndex++) {
                    if (this.formOptions.portfolioOfferings[completeOffIndex].portfolioName == offeringPortfolio) {
                        for (let offIndex = 0; offIndex < this.formOptions.portfolioOfferings[completeOffIndex].offeringName.length; offIndex++) {
                            const element = this.formOptions.portfolioOfferings[completeOffIndex].offeringName[offIndex];
                            if (this.formData.offering.indexOf(element) > -1)
                                counter = counter + 1
                        }
                    }
                }
                // console.log('offeringPortfolio:'+offeringPortfolio+':'+counter);
                if (counter == 0)
                    return false;
            }
       // }
        return true;
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

    getIndustriesState(value) {
        return this.formData.industry == value
    }

    getSectorState(value) {
        return this.formData.sector == value;
    }

    industriesChanged(checked, value) {
        if (checked) {
            this.formData.industry = value;
            this.formData.sector = "";
        }
        else {
            this.formData.industry = "";
            this.formData.sector = ""
        }
        this.setSectorList(this.formData.industry);
    }

    sectorChange(checked, value) {
        if (checked) {
            this.formData.sector = value;
        }
        else
            //this.formData.sector.splice(this.formData.sector.indexOf(value), 1);
            this.formData.sector = "";
        // console.log(this.formData.sector);
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
