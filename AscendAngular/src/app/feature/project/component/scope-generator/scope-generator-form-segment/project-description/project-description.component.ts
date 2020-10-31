import { HttpParams } from '@angular/common/http';
import { Component, EventEmitter, Input, OnInit, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { FormControl, NgForm, Validators } from '@angular/forms';
import { Observable } from 'rxjs';
import { debounceTime, finalize, switchMap, tap } from 'rxjs/operators';
import { User } from 'src/app/feature/project/constants/ascend-user-info';
import { ScopeGeneratorFormModel } from 'src/app/feature/project/model/project-scope-generator/scope-generator-form.model';
import { HttpServiceHelper } from 'src/app/types/common/HttpServiceHelper';
import { GeneratescopeService } from './../../../../service/generatescope.service';

@Component({
  selector: 'app-project-description',
  templateUrl: './project-description.component.html',
  styleUrls: ['./project-description.component.scss'],
  encapsulation: ViewEncapsulation.None,
})
export class ProjectDescriptionForm implements OnInit {

  @Input()
  formData: ScopeGeneratorFormModel;

  @Input() formOptions: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  @Input()
  isEditable: boolean;

  @ViewChild('projectForm', { static: false })
  ngForm: NgForm;

  showError: boolean = false;

  constructor(private projectService: GeneratescopeService
    , private httpService: HttpServiceHelper
  ) { }

  typeOfProject: string[] = ['Implementation', 'Operate', 'Upgrade'];

  searchManager = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
  searchLeadPD = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
  searchUsiEmd = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
  searchUsiGdm = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
  searchLeadQaPartner = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
  searchUsiQaReviewer = new FormControl('', [Validators.required, Validators.pattern('[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,4}$')]);
  isLoading = false;
  errorMsg: string;
  url = 'https://graph.microsoft.com/v1.0/users';
  filteredUser: any = [];
  filteredPDUser: any = [];
  filteredUsiEmd: any = [];
  filteredUsiGdm: any = [];
  filteredLeadQaPartner: any = [];
  filteredUsiQaReviewer: any = [];
  defaultPirmryFirm = 'United States';

  ngOnInit() {
    // this.formOptions = data;
     //defaulting primaryMemberFirm value
    let primaryMemberFirmArray: any = this.formOptions.primaryMemberFirm;
    for (let primaryIndex = 0; primaryIndex < primaryMemberFirmArray.length; primaryIndex++) {
      if (this.defaultPirmryFirm === this.formOptions.primaryMemberFirm[primaryIndex]) {
        this.formData.primaryMemberFirm = this.defaultPirmryFirm;
      }
    }
    this.formData.projectTypeDetails = this.formData.projectTypeDetails ? this.formData.projectTypeDetails : [{}];

    if (this.formData.primaryPortfolioOfferings) {
      this.setPrimaryOfferingList(this.formData.primaryPortfolioOfferings)
    }
    if (this.formData.secondaryPortfolioOfferings) {
      this.setSecondaryOfferingList(this.formData.secondaryPortfolioOfferings)
    }
    this.searchManager.setValue(this.formData.projectManager);
    this.searchLeadPD.setValue(this.formData.leadPD);
    this.searchUsiEmd.setValue(this.formData.usiEmd);
    this.searchUsiGdm.setValue(this.formData.usiGdm);
    this.searchLeadQaPartner.setValue(this.formData.leadQaPartner);
    this.searchUsiQaReviewer.setValue(this.formData.usiQaReviewer);


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

    this.searchUsiEmd.valueChanges
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
        this.filteredUsiEmd = [];
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
              this.filteredUsiEmd.push(user);
            }
            //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/
            if (this.filteredUsiEmd.length == 1) {
              this.setSelectedUsiEmd(user);
            }
          }
        } else {
          this.formData.usiEmd = '';
          this.formData.usiEmdfirstname = '';
          this.formData.usiEmdlastname = '';
          this.formData.usiEmdusername = '';
          this.formData.usiEmdjobtitle = '';
        }

      });

    this.searchUsiGdm.valueChanges
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
        this.filteredUsiGdm = [];
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
              this.filteredUsiGdm.push(user);
            }
            //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/
            if (this.filteredUsiGdm.length == 1) {
              this.setSelectedUsiGdm(user);
            }
          }
        } else {
          this.formData.usiGdm = '';
          this.formData.usiGdmfirstname = '';
          this.formData.usiGdmlastname = '';
          this.formData.usiGdmusername = '';
          this.formData.usiGdmjobtitle = '';
        }

      });

    this.searchUsiQaReviewer.valueChanges
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
        this.filteredUsiQaReviewer = [];
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
              this.filteredUsiQaReviewer.push(user);
            }
            //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/
            if (this.filteredUsiQaReviewer.length == 1) {
              this.setSelectedUsiQaReviewer(user);
            }
          }
        } else {
          this.formData.usiQaReviewer = '';
          this.formData.usiQaReviewerfirstname = '';
          this.formData.usiQaReviewerlastname = '';
          this.formData.usiQaReviewerusername = '';
          this.formData.usiQaReviewerjobtitle = '';
        }

      });

    this.searchLeadQaPartner.valueChanges
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
        this.filteredLeadQaPartner = [];
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
              this.filteredLeadQaPartner.push(user);
            }
            //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/
            if (this.filteredLeadQaPartner.length == 1) {
              this.setSelectedLeadQaPartner(user);
            }
          }
        } else {
          this.formData.leadQaPartner = '';
          this.formData.leadQaPartnerfirstname = '';
          this.formData.leadQaPartnerlastname = '';
          this.formData.leadQaPartnerusername = '';
          this.formData.leadQaPartnerjobtitle = '';
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

  setSelectedUsiEmd(user) {
    this.formData.usiEmd = user.userId;
    this.formData.usiEmdfirstname = user.ssoUser.givenName;
    this.formData.usiEmdlastname = user.ssoUser.surname;
    this.formData.usiEmdusername = user.ssoUser.displayName;
    this.formData.usiEmdjobtitle = user.ssoUser.jobTitle;
  }

  setSelectedUsiGdm(user) {
    this.formData.usiGdm = user.userId;
    this.formData.usiGdmfirstname = user.ssoUser.givenName;
    this.formData.usiGdmlastname = user.ssoUser.surname;
    this.formData.usiGdmusername = user.ssoUser.displayName;
    this.formData.usiGdmjobtitle = user.ssoUser.jobTitle;
  }

  setSelectedUsiQaReviewer(user) {
    this.formData.usiQaReviewer = user.userId;
    this.formData.usiQaReviewerfirstname = user.ssoUser.givenName;
    this.formData.usiQaReviewerlastname = user.ssoUser.surname;
    this.formData.usiQaReviewerusername = user.ssoUser.displayName;
    this.formData.usiQaReviewerjobtitle = user.ssoUser.jobTitle;
  }

  setSelectedLeadQaPartner(user) {
    this.formData.leadQaPartner = user.userId;
    this.formData.leadQaPartnerfirstname = user.ssoUser.givenName;
    this.formData.leadQaPartnerlastname = user.ssoUser.surname;
    this.formData.leadQaPartnerusername = user.ssoUser.displayName;
    this.formData.leadQaPartnerjobtitle = user.ssoUser.jobTitle;
  }

  projectTypeChanged(typeObj, subType) {

    if (this.formData.projectTypeDetails[0].projectType != typeObj.projectType) {
      this.formData.projectTypeDetails[0].projectType = typeObj.projectType;
      if (typeObj.typeDetails.length == 0) {
        this.formData.projectTypeDetails[0].typeDetails = [];
        this.formData.projectTypeDetails[0].typeDetails[0] = typeObj.projectType;
      }
      else
        this.formData.projectTypeDetails[0].typeDetails = this.formData.projectTypeDetails[0].typeDetails.filter(t => {
          return typeObj.typeDetails && typeObj.typeDetails.indexOf(t) >= 0;
        })
    }
    else {
      if (typeObj.projectType != 'Advise') {
        this.formData.projectTypeDetails[0].typeDetails = [];
        this.formData.projectTypeDetails[0].typeDetails[0] = subType;
      }
    }
  }

  setPrimaryOfferingList(value) {
    this.formOptions.primaryOfferings = [];
    // for (let index = 0; index < value.length; index++) {
    // let portfolioName = value[index];
    for (let portfolio of this.formOptions.primaryPortfolioOfferings) {
      if (portfolio.portfolioName === value) {
        let offeringNames = portfolio.offeringName || [];
        for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
          this.formOptions.primaryOfferings.push(offeringNames[offIndex]);
        }
      }
    }
  }

  setSecondaryOfferingList(value) {
    this.formOptions.secondaryOfferings = [];
     for (let index = 0; index < value.length; index++) {
    let portfolioName = value[index];
    for (let portfolio of this.formOptions.secondaryPortfolioOfferings) {
      if (portfolio.portfolioName === portfolioName) {
        let offeringNames = portfolio.offeringName || [];
        for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
          this.formOptions.secondaryOfferings.push(offeringNames[offIndex]);
        }
      }
    }
   }
  }
  onNext(clickedSegment?:any) {
    console.log('Project tab:nextClicked');
    this.showError = true;
    if (this.isValid()) {
      if(clickedSegment)
        this.next.emit({data:this.formData, clickedSegment: clickedSegment});
      else
        this.next.emit(this.formData);
      this.showError = false;
    }
    else{
      const firstElementWithError = document.querySelector('.ng-invalid,.multi-select-error-div');
      if(firstElementWithError)
        firstElementWithError.scrollIntoView({ behavior: 'smooth' });
    }
    event.preventDefault();
  }

  onPrev() {
    console.log('Project tab:prevClicked');
    this.prev.emit();
  }

  getPrimaryPortfolioOfferingsState(value) {
    if (this.formData.primaryPortfolioOfferings.indexOf(value) > -1)
      return true;
    else return false;
  }

  getPrimaryOfferingsState(value) {
    if (this.formData.primaryOffering.indexOf(value) > -1)
      return true;
    else return false;
  }

  getSecondaryOfferingsState(value) {
    if (this.formData.secondaryOffering.indexOf(value) > -1)
      return true;
    else return false;
  }

  getSecondaryPortfolioOfferingsState(value) {
    if (this.formData.secondaryPortfolioOfferings && this.formData.secondaryPortfolioOfferings.indexOf(value) > -1)
      return true;
    else return false;
  }

  primaryPortfolioOfferingsChanged(checked, value) {
    if (checked) {
      let index = this.formData.primaryPortfolioOfferings ? this.formData.primaryPortfolioOfferings.indexOf(value) : -1;
      if (index == -1) {
        //this.formData.primaryPortfolioOfferings.push(value);
        this.formData.primaryPortfolioOfferings = value;
        this.formData.primaryOffering = "";
        //For newly selected industry select all the related sectors as well.
        //this.setOfferingForPortfolio(value);
      }
    }
    else {
      // console.log(value);
      let portfolioOfferings = this.formOptions.primaryPortfolioOfferings;
      for (let index = 0; index < portfolioOfferings.length; index++) {
        if (portfolioOfferings[index].portfolioName == value) {
          let offeringNames = portfolioOfferings[index].offeringName;
          for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
            let offeringName = offeringNames[offIndex];
            let offeringIndex = this.formData.primaryOffering.indexOf(offeringName);
            if (offeringIndex != -1) {
              //this.formData.primaryOffering.splice(offeringIndex, 1);
              this.formData.primaryOffering = "";
            }
          }
        }

      }
      //this.formData.primaryPortfolioOfferings.splice(this.formData.primaryPortfolioOfferings.indexOf(value), 1);
      this.formData.primaryPortfolioOfferings = "";
    }
    this.setPrimaryOfferingList(this.formData.primaryPortfolioOfferings);
  }

  secondaryPortfolioOfferingsChanged(checked, value) {
    if (checked) {
      let index = this.formData.secondaryPortfolioOfferings ? this.formData.secondaryPortfolioOfferings.indexOf(value) : -1;
      if (index == -1) {
        this.formData.secondaryPortfolioOfferings.push(value);
        // this.formData.secondaryPortfolioOfferings = value;
        // this.formData.secondaryOffering = "";
        //For newly selected industry select all the related sectors as well.
        //this.setOfferingForPortfolio(value);
      }
    }
    else {
      // console.log(value);
      let portfolioOfferings = this.formOptions.secondaryPortfolioOfferings;
      for (let index = 0; index < portfolioOfferings.length; index++) {
        if (portfolioOfferings[index].portfolioName == value) {
          let offeringNames = portfolioOfferings[index].offeringName;
          for (let offIndex = 0; offIndex < offeringNames.length; offIndex++) {
            let offeringName = offeringNames[offIndex];
            let offeringIndex = this.formData.secondaryOffering.indexOf(offeringName);
            if (offeringIndex != -1) {
              this.formData.secondaryOffering.splice(offeringIndex, 1);
              //this.formData.secondaryOffering = "";
            }
          }
        }

      }
      this.formData.secondaryPortfolioOfferings.splice(this.formData.secondaryPortfolioOfferings.indexOf(value), 1);
      //this.formData.secondaryPortfolioOfferings = "";
    }
    this.setSecondaryOfferingList(this.formData.secondaryPortfolioOfferings);
  }

  isValid() {
    return (this.formData.primaryPortfolioOfferings.length && this.formData.primaryOffering.length && !this.searchManager.invalid && !this.searchLeadPD.invalid
      && !this.searchUsiEmd.invalid && !this.searchUsiGdm.invalid && !this.searchUsiQaReviewer.invalid && !this.searchLeadQaPartner.invalid && this.ngForm.valid && this.primaryPortfolioOfferingsCombination())
  }

  primaryPortfolioOfferingsCombination() {
    //for (let portfolioIndex = 0; portfolioIndex < this.formData.primaryPortfolioOfferings.length; portfolioIndex++) {
    if (this.formData.primaryPortfolioOfferings != '') {
      let offeringPortfolio = this.formData.primaryPortfolioOfferings;
      let counter: number = 0;
      for (let completeOffIndex = 0; completeOffIndex < this.formOptions.primaryPortfolioOfferings.length; completeOffIndex++) {
        if (this.formOptions.primaryPortfolioOfferings[completeOffIndex].portfolioName == offeringPortfolio) {
          for (let offIndex = 0; offIndex < this.formOptions.primaryPortfolioOfferings[completeOffIndex].offeringName.length; offIndex++) {
            const element = this.formOptions.primaryPortfolioOfferings[completeOffIndex].offeringName[offIndex];
            if (this.formData.primaryOffering.indexOf(element) > -1)
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

  secondaryPortfolioOfferingsCombination() {
    for (let portfolioIndex = 0; portfolioIndex < this.formData.secondaryPortfolioOfferings.length; portfolioIndex++) {
    if (this.formData.secondaryPortfolioOfferings[portfolioIndex] != '') {
      let offeringPortfolio = this.formData.secondaryPortfolioOfferings;
      let counter: number = 0;
      for (let completeOffIndex = 0; completeOffIndex < this.formOptions.secondaryPortfolioOfferings.length; completeOffIndex++) {
        if (this.formOptions.secondaryPortfolioOfferings[completeOffIndex].portfolioName == offeringPortfolio) {
          for (let offIndex = 0; offIndex < this.formOptions.secondaryPortfolioOfferings[completeOffIndex].offeringName.length; offIndex++) {
            const element = this.formOptions.secondaryPortfolioOfferings[completeOffIndex].offeringName[offIndex];
            if (this.formData.secondaryOffering.indexOf(element) > -1)
              counter = counter + 1
          }
        }
      }
      // console.log('offeringPortfolio:'+offeringPortfolio+':'+counter);
      if (counter == 0)
        return false;
    }
    }
    return true;
  }

  primaryOfferingChange(checked, value) {
    if (checked) {
      let index = this.formData.primaryOffering.indexOf(value);
      if (index == -1)
        //this.formData.primaryOffering.push(value);
        this.formData.primaryOffering = value;
    }
    else
      // this.formData.primaryOffering.splice(this.formData.primaryOffering.indexOf(value), 1);
      this.formData.primaryOffering = "";
    // console.log(this.formData.offering);
  }

  secondaryOfferingChange(checked, value) {
    if (checked) {
      let index = this.formData.secondaryOffering.indexOf(value);
      if (index == -1)
        this.formData.secondaryOffering.push(value);
        //this.formData.secondaryOffering = value;
    }
    else
      this.formData.secondaryOffering.splice(this.formData.secondaryOffering.indexOf(value), 1);
     // this.formData.secondaryOffering = "";
    // console.log(this.formData.offering);
  }

}
