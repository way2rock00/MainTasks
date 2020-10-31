import { CryptUtilService } from './../../../../shared/services/crypt-util.service';
import { Component, OnInit, Inject, ViewChild } from '@angular/core';
import { UserInfo } from '../../constants/ascend-user-project-info';
import { MatDialog } from '@angular/material/dialog';
import { ProjectMembersComponent } from '../project-members/project-members.component';
import { ProjectPassingInfoModel } from '../../model/project-passing-info.model';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { PassProjectInfoService } from '../../service/pass-project-info.service';
import { Router, ActivatedRoute, Params } from '@angular/router';
import { MatTable } from '@angular/material/table';
import { ProjectWorkspaceService } from '../../service/project-workspace.service';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';
import { FormControl } from '@angular/forms';
import { startWith, map } from 'rxjs/operators';
import { ProjectDetailsInfo } from '../../constants/ascend-project-details';
import { ManageAdminComponent } from './manage-admin/manage-admin.component';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { User } from '../../constants/ascend-user-info';
import { CommonDialogueBoxComponent } from 'src/app/shared/components/common-dialogue-box/common-dialogue-box.component';
import { DomSanitizer } from '@angular/platform-browser';


let userData: UserInfo[] = [{
  "userId": "mkothari@deloitte.com",
  "isAscendAdmin": "true",
  "projectInfo": [{
    "projectId": 1,
    "projectName": "Eaton",
    "clientName": "Eaton",
    "clientLogoURL": "www.google.com",
    "clientDesc": "Eaton test is long running project in office",
    "erpPackage": "SAP",
    "manager": "Rahul Trivedi",
    "projectRole": "PROJECT_MEMBER",
    "userroleId": "1"
  },
  {
    "projectId": 2,
    "projectName": "7-Eleven",
    "clientName": "7-Eleven",
    "clientLogoURL": "www.google.com",
    "clientDesc": "FMCG is the new client on the floor",
    "erpPackage": "Oracle",
    "manager": "Rahul Jha",
    "projectRole": "PROJECT_ADMIN",
    "userroleId": "2"
  },
  {
    "projectId": 3,
    "projectName": "Walmart",
    "clientName": "Walmart",
    "clientLogoURL": "www.google.com",
    "clientDesc": "Walmart is the retail client having lot of visibility",
    "erpPackage": "O2",
    "manager": "Rahul Singh",
    "projectRole": "PROJECT_MEMBER",
    "userroleId": "3"
  }]
},
{
  "userId": "viveksawant@deloitte.com",
  "isAscendAdmin": "false",
  "projectInfo": [{
    "projectId": 4,
    "projectName": "McD",
    "clientName": "McD",
    "clientLogoURL": "www.google.com",
    "clientDesc": "McD is longest running and biggest project in CIP Sector",
    "erpPackage": "SAP",
    "manager": "Rahul Malhotra",
    "projectRole": "PROJECT_ADMIN",
    "userroleId": "4"
  },
  {
    "projectId": 5,
    "projectName": "HLT",
    "clientName": "HLT",
    "clientLogoURL": "www.google.com",
    "clientDesc": "HLT is Banking client with facilities across the EMA region",
    "erpPackage": "Oracle",
    "manager": "Rahul Poojari",
    "projectRole": "PROJECT_MEMBER",
    "userroleId": "5"
  },
  {
    "projectId": 6,
    "projectName": "BMTY",
    "clientName": "BMTY",
    "clientLogoURL": "www.google.com",
    "clientDesc": "BMTY is telecom industry giant",
    "erpPackage": "O2",
    "manager": "Rahul Srivastava",
    "projectRole": "PROJECT_ADMIN",
    "userroleId": "6"
  },
  {
    "projectId": 7,
    "projectName": "Kroger",
    "clientName": "Kroger",
    "clientLogoURL": "www.google.com",
    "clientDesc": "Kroger is recent oracle cloud win",
    "erpPackage": "O2",
    "manager": "Rahul Vaidya",
    "projectRole": "PROJECT_MEMBER",
    "userroleId": "7"
  }]
}
];

@Component({
  selector: 'app-project-workspace',
  templateUrl: './project-workspace.component.html',
  styleUrls: ['./project-workspace.component.scss']
})
export class ProjectWorkspaceComponent implements OnInit {

  userInfo: UserInfo;
  constUserInfo: UserInfo;
  projectPassingInfo: ProjectPassingInfoModel = new ProjectPassingInfoModel();
  projectGlobalInfo: ProjectGlobalInfoModel = new ProjectGlobalInfoModel();
  userId: String;
  //searchClickedFlag: boolean;

  filteredOptions: Observable<string[]>;
  myControl = new FormControl();
  options: string[] = ['One', 'Two', 'Three'];

  err_object: any = { flag: false, error_msg: '' };

  @ViewChild(MatTable, { static: false }) table: MatTable<any>;
  searchbarEnable: boolean;
  menuOpen = -1;
  //constructor() { }
  constructor(public dialog: MatDialog,
    private data: PassProjectInfoService
    , private globalData: PassGlobalInfoService
    , private router: Router
    , private projectWorkspaceService: ProjectWorkspaceService
    , private messagingService: MessagingService
    , private cryptUtilService: CryptUtilService
    , private sanitizer: DomSanitizer

  ) { }

  modifyProjectDetails(projectId) {
    let url: string = '/project/update/' + projectId;
    this.router.navigate([url]);
  }

  getSafeURL(logoURL) {
    return this.sanitizer.bypassSecurityTrustResourceUrl(logoURL);
      }
    

  goToProjectsPage(projectId, mode, userId) {
    // console.log("Parent called param passing");
    // console.log('projectId:'+projectId);
    // console.log('mode:'+mode);
    // console.log('userId:'+userId);
    this.projectPassingInfo.projectId = projectId;
    this.projectPassingInfo.operation = mode;
    this.projectPassingInfo.userId = userId
    this.data.updateData(this.projectPassingInfo);
    //this.data.updateData('From Paranet');
    this.router.navigate(['/project/create']);
  }

  goToAscendPage(projectInfo, viewMode) {
    // console.log("Parent called param passing");
    // console.log('projectId: '+projectId);
    // console.log('isEditable: '+viewMode);
    // console.log('userroleId:'+userroleId);
    // console.log('projectName:'+projectName);
    // console.log('clientName:'+clientName);
    // this.projectGlobalInfo.projectId = projectId;
    // this.projectGlobalInfo.viewMode = viewMode;
    // this.projectGlobalInfo.uniqueId = userroleId;
    // this.projectGlobalInfo.projectName = projectName;
    // this.projectGlobalInfo.clientName = clientName;
    // this.projectGlobalInfo.clientUrl = clientURL;
    // this.projectGlobalInfo.role = role;
    this.setProject(projectInfo, viewMode)
    // this.globalData.updateGlobalData(this.projectGlobalInfo);
    //this.data.updateData('From Paranet');
    this.router.navigate(['/home']);
  }

  openDialog(projectId, projectName, manager): void {
    // console.log('Hi:'+this.projectPassingInfo.projectId);
    this.err_object.flag = false;
    this.err_object.error_msg = '';

    const dialogRef = this.dialog.open(ProjectMembersComponent, {
      width: '800px',
      data: { projectId: projectId, headerText: 'Manage teams', projectName: projectName, manager: manager }
    });

    dialogRef.afterClosed().subscribe(result => {
      // console.log('The dialog was closed');
      this.initializeData();
      //this.animal = result;
    });
  }

  initializeStaticList(array) {
    // console.log('Initializing List');
    this.staticList = [];
    for (let index = 0; index < array.length; index++) {
      let element = array[index];
      this.staticList.push(element.projectName);
    }
    this.filteredOptions = this.myControl.valueChanges
      .pipe(
        startWith(''),
        map(value => this._filter(value))
      );
  }

  deleteProjectClicked(projectinfo) {
    // console.log('Delete project clicked:'+projectinfo);
    // console.log(JSON.stringify(projectinfo));
    let projectDetailsList: ProjectDetailsInfo[] = [];
    let projectDetails: ProjectDetailsInfo = new ProjectDetailsInfo();
    projectDetails.action = 'DELETE';
    projectDetails.projectName = projectinfo.projectName;
    projectDetailsList.push(projectDetails);
    // console.log(JSON.stringify(projectDetailsList));
    let updateProjectInfoURL = environment.BASE_URL + '/projectdetailupload';
    this.projectWorkspaceService.updateProjectInformation(updateProjectInfoURL, projectDetailsList)
      .subscribe(
        (data) => {
          // console.log(JSON.stringify(data));
          let res: any = data;
          // console.log('Response Msg:'+res.MSG);
          if (res.MSG == 'SUCCESS') {
            // console.log('Index:'+this.userInfo.projectInfo.indexOf(projectinfo));
            let index = this.userInfo.projectInfo.indexOf(projectinfo);
            if (index > -1) {
              this.userInfo.projectInfo.splice(index, 1);
            }
            this.constUserInfo = JSON.parse(JSON.stringify(this.userInfo));
            //// console.log(JSON.stringify(this.userInfo));
            //// console.log(JSON.stringify(this.constUserInfo));
            this.initializeStaticList(this.userInfo.projectInfo);

            // console.log(this.staticList.length);
            // this.err_object.flag = true;
            // this.err_object.error_msg = 'Project has been deleted successfully';

            this.dialog.open(CommonDialogueBoxComponent, {
              data: {
                from: 'PROJECT WORKSPACE',
                message: 'Project "' + projectinfo.projectName + '" has been deleted successfully.'
              }
            });

          } else {
            // this.err_object.flag = true;
            // this.err_object.error_msg = 'Unable to delete the project.' + res.MSG;

            this.dialog.open(CommonDialogueBoxComponent, {
              data: {
                from: 'PROJECT WORKSPACE',
                message: 'Unable to delete "' + projectinfo.projectName + '" project. Error Message: ' + res.MSG + '.'
              }
            });

          }

        }
      );

  }

  public searchResult = '';
  public staticList = [];

  public handleStaticResultSelected(result) {
    this.searchResult = result;
    let projectFilter = this.searchResult;
    this.err_object.flag = false;
    this.err_object.error_msg = '';

    // console.log('In handleStaticResultSelected: projectFilter:'+projectFilter);
    if (projectFilter != '') {
      this.userInfo.projectInfo =
        this.constUserInfo.projectInfo.filter(function (projectObj) {
          return (projectObj.projectName).toUpperCase().indexOf(projectFilter.toUpperCase()) == -1 ? false : true;
        });
    } else {
      this.userInfo.projectInfo = JSON.parse(JSON.stringify(this.constUserInfo.projectInfo));
    }
    // console.log(JSON.stringify(this.userInfo.projectInfo));
  }

  public handleFocusOutEvent(result) {
    // console.log(JSON.stringify(this.userInfo.projectInfo));
    // console.log(JSON.stringify(this.constUserInfo.projectInfo));
    this.err_object.flag = false;
    this.err_object.error_msg = '';
    this.searchResult = result.target.value;
    let projectFilter = this.searchResult;
    // console.log('In handleFocusOutEvent: projectFilter:'+projectFilter);
    if (projectFilter != '') {


      this.userInfo.projectInfo =
        this.constUserInfo.projectInfo.filter(function (projectObj) {
          return (projectObj.projectName).toUpperCase().indexOf(projectFilter.toUpperCase()) == -1 ? false : true;
        });
    } else {
      this.userInfo.projectInfo = JSON.parse(JSON.stringify(this.constUserInfo.projectInfo));
    }
    // console.log(JSON.stringify(this.userInfo.projectInfo));
  }

  /*searchClicked(){
    this.searchClickedFlag = true;
  }*/
  initialConditions(projectInfo) {
    if (projectInfo == undefined || projectInfo == null || projectInfo.length == 0) {
      this.router.navigate(['/home']);
    } else {
      if (projectInfo.find(t => t.projectRole == 'PROJECT_ADMIN') != undefined) {
        this.searchbarEnable = true;
      }
    }
  }

  initializeData() {
    //JSON Working Code End
    let userProjectInfoURL = environment.BASE_URL + '/userInfo/' + this.userId;
    // console.log('userProjectInfoURL:'+userProjectInfoURL);
    this.projectWorkspaceService.getUserProjectInfo(userProjectInfoURL).subscribe(
      (data) => {
        // console.log('Data Received');
        // console.log(data);
        if (data != null) {
          this.initialConditions(data[0].projectInfo);
          this.constUserInfo = JSON.parse(JSON.stringify(data[0]));//  data[0];
          // console.log(this.constUserInfo);
          this.userInfo = JSON.parse(JSON.stringify(this.constUserInfo));
          this.initializeStaticList(this.userInfo.projectInfo);
          /*
          Old approach to initialize list.
          for (let index = 0; index < this.userInfo.projectInfo.length; index++) {
            let element = this.userInfo.projectInfo[index];
            this.staticList.push(element.projectName);
          }   */
        }
      }
    );

    this.filteredOptions = this.myControl.valueChanges
      .pipe(
        startWith(''),
        map(value => this._filter(value))
      );

  }

  getClientLogo(projectId) {
    console.log('projectId:' + projectId);
    projectId = "7";
    let clientLogoServiceURL = environment.BASE_URL + '/getClientLogo/' + projectId;
    this.projectWorkspaceService.getClientLogo(clientLogoServiceURL).subscribe(
      (data) => {
        console.log('Data from getClientLogo:' + clientLogoServiceURL + ' data:' + data);
      });

  }

  ngOnInit() {

    localStorage.clear();
    this.cryptUtilService.sessionClear();

    this.projectGlobalInfo.viewMode = "EXPLORE";
    this.projectGlobalInfo.projectId = "0";
    this.projectGlobalInfo.uniqueId = "0";
    this.projectGlobalInfo.projectName = "";
    this.projectGlobalInfo.clientName = "";
    this.projectGlobalInfo.clientUrl = "";
    this.globalData.updateGlobalData(this.projectGlobalInfo);

    this.messagingService.subscribe(BUS_MESSAGE_KEY.USER_DETAILS, (data: User) => {
      if (data) {
        this.userId = data.userId;
        // console.log(data);
        this.initialConditions(data.projectDetails.projectInfo);
        if (this.userId) {
          this.initializeData();
        }
      }
    });
  }

  private _filter(value: string): string[] {
    const filterValue = value.toLowerCase();
    // console.log(filterValue);
    // console.log(this.staticList);
    return this.staticList.filter(option => option.toLowerCase().includes(filterValue));
  }

  getUserData(userId) {
    let userInfo: UserInfo;
    for (let index = 0; index < userData.length; index++) {
      let element = userData[index];
      if (element.userId == userId) {
        this.userInfo = userData[index];
        return this.userInfo
      }
    }
  }

  manageAdmin() {
    const dialogRef = this.dialog.open(ManageAdminComponent, {
      width: '800px',
      data: {}
    });

    dialogRef.afterClosed().subscribe(result => {
      this.initializeData();
    });
  }


  goToAscend() {
    //Values would be EXPLORE and PROJECT.
    this.projectGlobalInfo.viewMode = "EXPLORE";
    this.projectGlobalInfo.projectId = "0";
    this.projectGlobalInfo.uniqueId = "0";
    this.projectGlobalInfo.projectName = "";
    this.projectGlobalInfo.clientName = "";
    this.projectGlobalInfo.clientUrl = "";
    this.globalData.updateGlobalData(this.projectGlobalInfo);
    this.router.navigate(['/home']);
  }

  setProject(projectInfo, viewMode){
    this.projectGlobalInfo.projectId = projectInfo.projectId;
    this.projectGlobalInfo.viewMode = viewMode;
    this.projectGlobalInfo.uniqueId = projectInfo.userroleId;
    this.projectGlobalInfo.projectName = projectInfo.projectName;
    this.projectGlobalInfo.clientName = projectInfo.clientName;
    this.projectGlobalInfo.clientUrl = projectInfo.clientLogoURL;
    this.projectGlobalInfo.role = projectInfo.projectRole;
    this.cryptUtilService.setItem('IS_PSG_COMPLETE_FLAG', projectInfo.isPSGCompleted,'SESSION');
    this.globalData.updateGlobalData(this.projectGlobalInfo);
  }

  navigateToPsg(projectInfo, viewMode){
    this.setProject(projectInfo, viewMode)
    this.router.navigate(['project/psg/' + projectInfo.projectId]);
  }
}


