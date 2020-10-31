import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: 'root'
})
export class DeployTabDataService {

  deployContentsJson: any[];
  // deployURL: string = `${environment.BASE_URL}/deploy/misc/`;
  // deployTabName: string = "Deploy";
  // deployStorage: string = "DEPLOYJSONBKP";

  constructor(private http: HttpClient, private utilService: UtilService) {
    this.clearData();
  }

  getTabDataURL(URL): Observable<any> {
    return this.http.get<any>(`${environment.BASE_URL}${URL}${this.utilService.setfilterParamsURL()}`);
  }

  setSelectedFilter(e): Observable<any> {

    this.clearData();

    if (this.utilService.isGlobalFilter(e.data.type)) {
      this.utilService.setSelectedFilter(e)
    }

    return of(e.data);
  }

  // filterTabData(): Observable<any> {

  //   this.getTabDataURL(this.deployURL).subscribe(data => {
  //     this.deployContentsJson = this.utilService.formTabContents(data, this.deployTabName, this.deployStorage); 
  //   });

  //   return of();
  // }

  // updateDeployContents(data, projectId): Observable<any> {
  //   return this.http.post(this.deployURL + projectId, data);
  // }


  clearData() {
    this.deployContentsJson = [];
  }
}
