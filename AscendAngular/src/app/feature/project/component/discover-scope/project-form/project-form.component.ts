import { Component, OnInit } from '@angular/core';
import { NgForm, FormControl, Validators } from '@angular/forms';
import { ProjectScopeForm } from '../../../model/scope-generator-form.model';
import { tap, switchMap, finalize } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { HttpParams } from '@angular/common/http';
import { HttpServiceHelper } from 'src/app/types/common/HttpServiceHelper';
import { User } from 'src/app/feature/project/constants/ascend-user-info';
import { Router } from '@angular/router';
import { SharedService } from 'src/app/shared/services/shared.service';
import { environment } from 'src/environments/environment';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { GeneratescopeService } from '../../../service/generatescope.service';
import { MatDialog } from '@angular/material';

@Component({
  selector: 'app-project-form',
  templateUrl: './project-form.component.html',
  styleUrls: ['./project-form.component.scss']
})
export class ProjectFormComponent implements OnInit {

  ngForm: NgForm;

  step = 0;
  formData = new ProjectScopeForm();
  emailNodes = {
    usiEmd:{
      firstName: 'usiEmdfirstname',
      lastName: 'usiEmdlastname',
      userName: 'usiEmdusername',
      jobTitle: 'usiEmdjobtitle'
    },
    usiGdm:{
      firstName: 'usiGdmfirstname',
      lastName: 'usiGdmlastname',
      userName: 'usiGdmusername',
      jobTitle: 'usiGdmjobtitle'
    },
    usiQaReviewer:{
      firstName: 'usiQaReviewerfirstname',
      lastName: 'usiQaReviewerlastname',
      userName: 'usiQaReviewerusername',
      jobTitle: 'usiQaReviewerjobtitle'
    },
    leadQaPartner:{
      firstName: 'leadQaPartnerfirstname',
      lastName: 'leadQaPartnerlastname',
      userName: 'leadQaPartnerusername',
      jobTitle: 'leadQaPartnerjobtitle'
    }
  }

  formOptions: any;  
  // Search USI EMD
  searchUsiEmd = new FormControl('', [Validators.required, Validators.pattern('[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}$')]);
  filteredUser: any = [];
  isLoading: boolean = false;
  // Search USI GDM
  searchUsiGdm = new FormControl('', [Validators.required, Validators.pattern('[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}$')]);
  filteredUserGdm: any = [];
  isLoadingGdm: boolean = false;
  // Search Lead QA
  searchLeadQA = new FormControl('', [Validators.required, Validators.pattern('[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}$')]);
  filteredUserLeadQA: any = [];
  isLoadingLeadQA: boolean = false;
  // Search QAReviewer
  searchQAReviewer = new FormControl('', [Validators.required, Validators.pattern('[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}$')]);
  filteredUserQAReviewer: any = [];
  isLoadingQAReviewer: boolean = false;

  projectGlobalInfo: ProjectGlobalInfoModel;

  url = 'https://graph.microsoft.com/v1.0/users';

  constructor(private httpService: HttpServiceHelper, private router: Router, private sharedService: SharedService
    ,private globalData: PassGlobalInfoService, private generateScope: GeneratescopeService,public dialog: MatDialog) { }

  ngOnInit() {

    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
    });

    this.sharedService.getData(`${environment.BASE_URL}/projectdetailsPSGInfo/` + this.projectGlobalInfo.projectId).subscribe(res => {
      this.formOptions = res[0];
    });

    this.sharedService.getData(`${environment.BASE_URL}/projectdetailsPSG/` + this.projectGlobalInfo.projectId).subscribe(res => {
      this.formData = res[0];
    });

    this.searchUsiEmd.valueChanges.debounceTime(500).pipe(
      tap(() => {
        this.isLoading = true;
      }),
      switchMap(value => { return this.getGraphUsers(value) })
    ).subscribe(data => { this.filteredUser = this.setUserList(data, 'usiEmd')});

    this.searchUsiGdm.valueChanges.debounceTime(500).pipe(
      tap(() => {
        this.isLoading = true;
      }),
      switchMap(value => { return this.getGraphUsers(value) })
    ).subscribe(data => { this.filteredUserGdm = this.setUserList(data, 'usiGdm')});

    this.searchLeadQA.valueChanges.debounceTime(500).pipe(
      tap(() => {
        this.isLoading = true;
      }),
      switchMap(value => { return this.getGraphUsers(value) })
    ).subscribe(data => { this.filteredUserLeadQA = this.setUserList(data, 'leadQaPartner')});

    this.searchQAReviewer.valueChanges.debounceTime(500).pipe(
      tap(() => {
        this.isLoading = true;
      }),
      switchMap(value => { return this.getGraphUsers(value) })
    ).subscribe(data => { this.filteredUserQAReviewer = this.setUserList(data, 'usiQaReviewer')});
  }

  getGraphUsers(value) {
    if (value !== '') {
      return this.httpService.httpGetRequestWithParams(this.url, new HttpParams().set('$filter', "((startswith(displayName,'" + value + "') or startswith(mail,'" + value + "')) and userType eq 'Member')"))
        .pipe(
          finalize(() => {
            this.isLoading = false;
          }),
        )
    }
    else {
      return Observable.of(false);
    }
  }

  setUserList(datas: any, entity) {

    let filteredArray = [];

    if (datas) {
      let obj = datas;
      for (let index = 0; index < obj.value.length; index++) {
        const user: User = new User();
        let email = obj.value[index].mail;
        if (email && (email.includes('deloitte.') || email.includes('DELOITTE.'))) {
          user.userId = obj.value[index].mail;
          user.userName = obj.value[index].displayName;
          user.ssoUser.displayName = obj.value[index].displayName;
          user.ssoUser.givenName = obj.value[index].givenName;
          user.ssoUser.surname = obj.value[index].surname;
          user.ssoUser.jobTitle = obj.value[index].jobTitle;
          
          filteredArray.push(user);
        }
        //If somebody keeps on typing emailid, formdata was not getting set and thus setting the formData was not getting set/ 
        if (filteredArray.length == 1) {
          this.setSelectedManager(user, entity);
        }
      }
    } else {
      this.formData[entity] = '';
    }

    return filteredArray;
  }

  navigate(route){
    this.router.navigate([route]);
  }

  setSelectedManager(user, entity) {
    
    this.formData[entity] = user.userId;
    let entityNode = this.emailNodes[entity];
    this.formData[entityNode.firstName] = user.ssoUser.givenName;
    this.formData[entityNode.lastName] = user.ssoUser.surname;
    this.formData[entityNode.userName] = user.ssoUser.displayName;
    this.formData[entityNode.jobTitle] = user.ssoUser.jobTitle;
  }
}
