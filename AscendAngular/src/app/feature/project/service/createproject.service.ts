import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { map } from 'rxjs/operators';
import { of, Observable } from 'rxjs';
import { ProjectFormModel } from '../model/add-edit-project/project-form.model';
import { PROJECT_API_ACTIONS } from '../constants/project-common';
import { ApiProjectModel } from '../model/add-edit-project/api-project-model';

@Injectable({
  providedIn: 'root'
})
export class CreateprojectService {

  readonly PROJECT_DETAILS_URL = '/projectDetailsPage';
  readonly CLIENT_DETAILS_URL = '/clientDetailsPage';
  readonly SCOPE_DETAILS_URL = '/scopeDetailsPage';
  readonly SAVED_PROJECT_DETAILS = '/projectdetails';
  readonly UPDATE_PROJECT_DETAILS = '/projectdetailupload';

  projectDetailsData: any;
  clientDetailsData: any;
  scopeDetailsData: any;

  constructor(private http : HttpClient) {}

  fetchProjectDetails(): Observable<any> {
    if (this.projectDetailsData) {
      return of(this.projectDetailsData);
    } else {
      return this.http.get(`${environment.BASE_URL}${this.PROJECT_DETAILS_URL}`)
      .pipe(
        map(data=>{
          this.projectDetailsData = data;
          return data;
        })
      );
    }
  }

  fetchClientDetails(): Observable<any> {
    if (this.clientDetailsData) {
      return of(this.clientDetailsData);
    } else {
      return this.http.get(`${environment.BASE_URL}${this.CLIENT_DETAILS_URL}`)
      .pipe(
        map(data=>{
          this.clientDetailsData = data;
          return data;
        })
      );
    }
  }

  fetchScopeDetails(): Observable<any> {
    if (this.scopeDetailsData) {
      return of(this.scopeDetailsData);
    } else {
      return this.http.get(`${environment.BASE_URL}${this.SCOPE_DETAILS_URL}`)
      .pipe(
        map(data=>{
          this.scopeDetailsData = data;
          return data;
        })
      );
    }
  }

  fetchSavedProjectData(projectId: String): Observable<ApiProjectModel> {
    return this.http.get<ApiProjectModel>(`${environment.BASE_URL}${this.SAVED_PROJECT_DETAILS}/${projectId}`)
      .pipe(map( data => data[0]));
  }

  updateProjectData(projectFormModel: ProjectFormModel, action) {
    let projectSaveAPIBody = new ApiProjectModel(projectFormModel);
    projectSaveAPIBody.action = action;
    let projectSaveAPIArr: any[]=[];
    projectSaveAPIArr.push(projectSaveAPIBody);
    console.log('**********PROJECT SAVE API CALL*******')  
    console.log(JSON.stringify(projectSaveAPIArr));
    var formData = new FormData();

    //Only if file has been uploaded then call append.
    if(projectFormModel.projectDetails.logoConsentFlag && projectFormModel.projectDetails.logoFile )
    formData.append("file",projectFormModel.projectDetails.logoFile);

    formData.append("data",JSON.stringify(projectSaveAPIArr));
    return this.http.post(`${environment.BASE_URL}${this.UPDATE_PROJECT_DETAILS}`, formData);

  }

}
