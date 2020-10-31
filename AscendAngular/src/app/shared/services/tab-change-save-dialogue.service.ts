import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class TabChangeSaveDialogueService {

  projectGlobalInfo: ProjectGlobalInfoModel;

  constructor(private http: HttpClient, private globalData: PassGlobalInfoService,) {
    globalData.share.subscribe( data => {
      this.projectGlobalInfo = data;
    })
   }

  updateTabContents(data, URL): Observable<any> {
    return this.http.post( `${environment.BASE_URL}${URL}${this.projectGlobalInfo.projectId}`, data);
  }
}
