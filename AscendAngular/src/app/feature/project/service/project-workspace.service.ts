import { Injectable } from '@angular/core';
import { HttpClient} from '@angular/common/http';
import { UserInfo } from 'src/app/shared/constants/ascend-user-project-info';
import { ProjectMemberInfo } from 'src/app/feature/project/constants/ascend-project-member-info';


@Injectable({
  providedIn: 'root'
})
export class ProjectWorkspaceService {

  constructor(private http : HttpClient) {}

  getUserProjectInfo(URL){
    // console.log('In ProjectWorkspaceService:'+URL);
    return this.http.get<UserInfo[]>(URL);
  }

  getProjectMembersInfo(URL){
    // console.log('In ProjectWorkspaceService:'+URL);
    return this.http.get<ProjectMemberInfo[]>(URL);
  }

  updateProjectInformation(URL,projectDetails){
    // console.log('In updateProjectInformation:'+URL);
    // console.log(projectDetails);
    // console.log(JSON.stringify(projectDetails));
    var formData = new FormData();
    formData.append("data",JSON.stringify(projectDetails));
    return this.http.post(URL,formData);
  }


  updateProjectMemberInformation(URL,projectMemberInfo){
    // console.log('In updateProjectMemberInformation:'+URL);
     console.log(JSON.stringify(projectMemberInfo));
    return this.http.post(URL,projectMemberInfo);
  }

  getUserList(URL){
   // console.log('In getUserList service:'+URL);
   return this.http.get<any[]>(URL);
  }

  getClientLogo(URL){
    // console.log('In ProjectWorkspaceService:'+URL);
    return this.http.get<ProjectMemberInfo[]>(URL);
  }  

}
