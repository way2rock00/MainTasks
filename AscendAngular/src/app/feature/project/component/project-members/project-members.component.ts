import { Component, Inject , ViewChild , OnInit} from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA} from '@angular/material/dialog';
import { MatTable} from '@angular/material/table';

import {ProjectMemberInfo} from '../../constants/ascend-project-member-info';
import { User } from '../../constants/ascend-user-info';
import { MembersInfo } from '../../constants/ascend-member-type';
import { ProjectWorkspaceService } from '../../service/project-workspace.service';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';
import { FormControl } from '@angular/forms';
//import { startWith, map } from 'rxjs/operators';
import { debounceTime, tap, switchMap, finalize, startWith, map } from 'rxjs/operators';

import { BroadcastService } from '@azure/msal-angular';
import { MsalService } from '@azure/msal-angular';
import { HttpClient, HttpParams } from '@angular/common/http';
import { HttpServiceHelper } from '../../../../types/common/HttpServiceHelper';
import { Subscription } from 'rxjs/Subscription';


let member_data: ProjectMemberInfo[] =[];


let usersList:  User[] = [];

@Component({
  selector: 'app-project-members',
  templateUrl: './project-members.component.html',
  styleUrls: ['./project-members.component.scss']
})
export class ProjectMembersComponent implements OnInit  {

  dataSource : ProjectMemberInfo = new ProjectMemberInfo();
  serviceDataSource : ProjectMemberInfo = new ProjectMemberInfo();
  //selectedMember : User = new User();
  selectMemberUserId: String;
  errorFlag: boolean;
  errorMessage:String;
  isFormReadOnly: boolean;
  showDeleteConfirmationBox:boolean;
  showSaveConfirmationBox:boolean;

  dialogHeader: String = '';
  projectName:String = '';
  manager:String = '';

  deletedUserObj : MembersInfo = new MembersInfo();

  displayedColumns: string[] = ['name', 'email', 'role','action'];
  @ViewChild(MatTable,{static: false}) table: MatTable<any>;
  usersCompleteList: User[]=[]//=usersList;

  filteredOptions: Observable<User[]>;
  myControl = new FormControl();


  roleNames:any [] = [{roleCode:'PROJECT_MEMBER'
                    ,roleName:'Member'},
                    {roleCode:'PROJECT_ADMIN'
                    ,roleName:'Admin'}
                   ];

  projectMemberInfo : ProjectMemberInfo = new ProjectMemberInfo();

  private subscription: Subscription;
  url = 'https://graph.microsoft.com/v1.0/users';
  filteredUser: any;
  searchUsersCtrl = new FormControl();
  isLoading = false;
  errorMsg: string;

  //Initialize the table.
  ngOnInit() {
    // console.log('******Called*******'+this.data.projectId+':'+member_data.length);

    //Service call starts.

    let projectMemberURL = environment.BASE_URL+'/projectmembers/'+this.data.projectId;
    // console.log('projectMemberURL:'+projectMemberURL);
    this.projectWorkspaceService.getProjectMembersInfo(projectMemberURL).subscribe(
      (data)=>{
        if(data != null){
          // console.log('Printing Member data from service');
          //console.log(JSON.stringify(data));
          //Adding below code...can be commeneted.
          member_data = data;
          this.dataSource=(member_data||[])[0];
          /*
          // console.log('TEMP');
          // console.log(member_data);
          for (let index = 0; index < member_data.length; index++) {
            const element = JSON.parse(JSON.stringify(member_data[index]));
            //// console.log('element.projectId:'+element.projectId);
            if(element.projectId == this.data.projectId){
              this.dataSource=element;
            }
          }*/
          this.serviceDataSource = JSON.parse(JSON.stringify(this.dataSource));
          // console.log(this.dataSource);
          // console.log(this.serviceDataSource);
        }
      }
    );

    /*let userListURL = environment.BASE_URL+'/userlist';
    this.projectWorkspaceService.getUserList(userListURL)
    .subscribe(
      (data)=>{
        for (let index = 0; index < data.length; index++) {
          let element = data[index];
          this.usersCompleteList.push(element);

        }
        // console.log(JSON.stringify(this.usersCompleteList));
      }
    );*/
    //Service call end.

    this.dialogHeader=this.data.headerText;
    this.projectName = this.data.projectName;
    this.manager = this.data.manager;

    //Local JSON Working Code
    /*
    for (let index = 0; index < member_data.length; index++) {
      const element = JSON.parse(JSON.stringify(member_data[index]));
      //// console.log('element.projectId:'+element.projectId);
      if(element.projectId == this.data.projectId){
        this.dataSource=element;
      }
    }
    this.serviceDataSource = JSON.parse(JSON.stringify(this.dataSource));
    // console.log(this.dataSource);
    // console.log(this.serviceDataSource);*/

    //Initializing initial values.
    this.errorFlag = false;
    this.isFormReadOnly = false;
    this.showDeleteConfirmationBox = false;
    this.showSaveConfirmationBox = false;

    /*this.filteredOptions = this.myControl.valueChanges
    .pipe(
      startWith(''),
      map(value => this._filter(value))
    );*/

    this.searchUsersCtrl.valueChanges
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
        //console.log('inside subscribe');
        const usersList: User[] = [];
        if (datas) {
          let obj = datas;
          // tslint:disable-next-line: prefer-for-of
          for (let index = 0; index < obj.value.length; index++) {
            const user: User = new User();
            let email = obj.value[index].mail;
            console.log(' Email:'+email);
            if(email && (email.includes('deloitte.') || email.includes('DELOITTE.')) ){
                user.userId = obj.value[index].mail;
                user.userName = obj.value[index].displayName;
                user.ssoUser.displayName = obj.value[index].displayName;
                user.ssoUser.givenName = obj.value[index].givenName;
                user.ssoUser.surname = obj.value[index].surname;
                user.ssoUser.jobTitle = obj.value[index].jobTitle;            
                usersList.push(user);
            }
          }
        }
        this.filteredUser = usersList;
        //console.log(usersList);
        //console.log(this.filteredUser);
      });
  }

  /*private _filter(value: string): User[] {
    const filterValue = value.toLowerCase();
    // console.log(this.usersCompleteList);
    return this.usersCompleteList.filter(option => option.userName.toLowerCase().includes(filterValue)
    || option.userId.toLowerCase().includes(filterValue));
  }*/

  handleFocusOutEvent(event){
    //// console.log(JSON.stringify(event));
    // console.log(event.srcElement.value);
    this.selectMemberUserId =event.srcElement.value;
  }

  constructor(
    public dialogRef: MatDialogRef<ProjectMembersComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any,
    private projectWorkspaceService:ProjectWorkspaceService,
    private httpService: HttpServiceHelper,
    ) {
    }

  onNoClick(): void {
    this.errorFlag = false;
    this.dialogRef.close();
  }

  saveClickedOnce(){
    // console.log('Overloaded save clicked');
    this.errorFlag = false;
    this.isFormReadOnly = true;
    this.showSaveConfirmationBox=true;
  }

  saveCancelClicked(){
    this.errorFlag = false;
    this.isFormReadOnly = false;
    this.showSaveConfirmationBox=false;
  }

  saveClickedSecond(){
    if(this.validateDataToSave()){
        let projectMemberUpdateURL = environment.BASE_URL+'/membersupdate';
        // console.log('projectMemberUpdateURL:'+projectMemberUpdateURL);
        let serviceDataSourceList : ProjectMemberInfo [] = [];
        //console.log(JSON.stringify(serviceDataSourceList));
        serviceDataSourceList.push(this.serviceDataSource);
        this.projectWorkspaceService.updateProjectMemberInformation(projectMemberUpdateURL,serviceDataSourceList)
        .subscribe(
          (data)=>{
            let res :any = data;
            // console.log('Response Msg:'+res.MSG);
            if(res.MSG=='SUCCESS'){
              this.errorFlag = false;
              this.isFormReadOnly = false;
              this.showSaveConfirmationBox=false;
              this.dialogRef.close();
            }else{
              this.errorFlag = true;
              this.errorMessage='Error while saving.'+res.MSG;
              this.isFormReadOnly = false;
              this.showSaveConfirmationBox=false;
            }
          }
        );
    }else{
      this.errorFlag = true;
      this.errorMessage='Error while saving. Please make some member as admin.';
      this.isFormReadOnly = false;
      this.showSaveConfirmationBox=false;
    }


  }

  //This validation is not make sure that there is atleast one team admin in the project. This validation is to avoid
  //deletion of Admin from project accidentally.
  validateDataToSave(){
    let teamMemberList = this.serviceDataSource.members;
    for (let index = 0; index < teamMemberList.length; index++) {
      let teamMember = teamMemberList[index];
      if(teamMember.projectRole == 'PROJECT_ADMIN' &&
         (teamMember.action == 'CREATE' ||teamMember.action == 'UPDATE'))
      return true;
    }
    return false;
  }

  /*getUserName(userId){
    for (let index = 0; index < this.usersCompleteList.length; index++) {
      let element = this.usersCompleteList[index];
      if(element.userId==userId)
      return element.userName;

    }
    return '';
  }*/

  addMember(){
      //console.log('In Add Member');
      //console.log(this.filteredUser);
      if(this.selectMemberUserId != ''){
      let selectedUserInfo : MembersInfo = new MembersInfo();
      selectedUserInfo.userId=this.filteredUser[0].userId;
      for (let index = 0; index < this.dataSource.members.length; index++) {
        let m1 = this.dataSource.members[index];
        if(m1.userId == selectedUserInfo.userId){
          this.errorFlag = true;
          this.errorMessage='Member already exists.';
          return;
        }
      }
      selectedUserInfo.userName= this.filteredUser[0].userName;
      selectedUserInfo.action='CREATE';
      selectedUserInfo.projectRole='PROJECT_ADMIN';
      selectedUserInfo.firstName=this.filteredUser[0].ssoUser.givenName;
      selectedUserInfo.lastName=this.filteredUser[0].ssoUser.surname;
      selectedUserInfo.displayName=this.filteredUser[0].ssoUser.displayName;
      selectedUserInfo.jobTitle=this.filteredUser[0].ssoUser.jobTitle;



      this.dataSource.members.push(selectedUserInfo);
      this.serviceDataSource.members.push(selectedUserInfo);
      this.errorFlag = false;
      this.table.renderRows();
      //console.log('In Add');
      //console.log(JSON.stringify(selectedUserInfo))
      //console.log(JSON.stringify(this.dataSource))
      console.log(JSON.stringify(this.serviceDataSource))      
    }

  }

  onRoleChange(event,row){
    this.errorFlag = false;
    // console.log(event);
    // console.log(event.value);
    // console.log(JSON.stringify(this.serviceDataSource));
    if(this.validateManagerDetail(this.manager,row.userId)){
      for (let index = 0; index < this.serviceDataSource.members.length; index++) {
        let m = this.serviceDataSource.members[index];
        if(m.userId == row.userId){
          this.serviceDataSource.members[index].projectRole = event.value;
        }
      }
    }
    // console.log(JSON.stringify(this.serviceDataSource));
    //console.log('In Role Change event');
    //console.log(JSON.stringify(this.dataSource))
    //console.log(JSON.stringify(this.serviceDataSource))      
  }

  removeClickedSecond(){
    this.errorFlag = false;

    this.isFormReadOnly = false;
    this.showDeleteConfirmationBox=false;

    let row = this.deletedUserObj;
    const index = this.dataSource.members.indexOf(row, 0);
    if (index > -1) {
      this.dataSource.members.splice(index, 1);
    }
    this.table.renderRows();
    // console.log(row);
    // console.log(JSON.stringify(this.serviceDataSource));
    for (let index = 0; index < this.serviceDataSource.members.length; index++) {
      let m = this.serviceDataSource.members[index];
      if(m.userId == row.userId){
        this.serviceDataSource.members[index].action = 'DELETE';
      }
    }
    // console.log(JSON.stringify(this.serviceDataSource));
    //console.log('In Delete event');
    //console.log(JSON.stringify(this.dataSource))
    //console.log(JSON.stringify(this.serviceDataSource))      
  }

  removeCancelClicked(){
    this.errorFlag = false;
    this.isFormReadOnly = false;
    this.showDeleteConfirmationBox=false;
  }

  removeClickedOnce(row){
    //console.log('Overloaded remove clicked');
    //console.log(row)
    if(this.validateManagerDetail(this.manager,row.userId)){
    this.errorFlag = false;
    this.isFormReadOnly = true;
    this.showDeleteConfirmationBox=true;
    this.deletedUserObj = row;
    }
  }

  //This function will validate that we are not trying to remove person who is manager on the project.
  //That should be done from Manager Project Details page in frontend.
  validateManagerDetail(manager,removedManager):boolean{
    if(manager==removedManager){
      this.errorFlag=true;
      this.errorMessage='Cannot remove/change Project Manager from Manage Team.Please change Manager from Modify Project Details screen.'
    return false;
    }
    else return true;
  }

  printArray(dataSource){
    /*for (let index = 0; index < this.dataSource.length; index++) {
      let currMemeber = this.dataSource[index];
      // console.log('New Print:'+currMemeber.email+':'+currMemeber.name+":"+currMemeber.role);
    }*/
  }

  ngOnDestroy() {
  }

}
