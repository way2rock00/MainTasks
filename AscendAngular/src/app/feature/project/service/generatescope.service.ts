import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class GeneratescopeService {

  readonly SAVED_PROJECT_DETAILS = '/projectdetailsPSG';
  readonly UPDATE_PROJECT_DETAILS = '/projectdetailsPSGInfo';
  readonly ALL_PSG_DATA = '/projectDetailsPagePSG'

  allPSGData: any;

  constructor(private http: HttpClient) { }

  fetchSavedProjectData(projectId: string): Observable<any> {
    return this.http.get<any>(`${environment.BASE_URL}${this.SAVED_PROJECT_DETAILS}/${projectId}`)
      .pipe(map(data => data[0]));
  }

  generateData() {
    var postData = [{
      "projectType": [],
      "scopes": [],
      "technicalScope": [],
      "implementationApproach": [],
      "phasePlanning": []
    }];
    return postData;
  }

  updateProjectScopeData(projectFormModel: any, action, projectId) {
    projectFormModel.action = action;
    return this.http.post(`${environment.BASE_URL}${this.UPDATE_PROJECT_DETAILS}/${projectId}`, projectFormModel);
  }

  fetchAllPSGData(projectId: string): Observable<any> {
    return this.http.get(`${environment.BASE_URL}${this.ALL_PSG_DATA}/${projectId}`)
      .pipe(
        map(data => {
          this.allPSGData = data;
          return data;
        })
      );
  }

  formatData(key, value) {
    var dataArray = [];
    var obj = {};
    obj['key'] = key;
    obj['value'] = value;
    dataArray.push(obj);
    return dataArray;
  }

}
