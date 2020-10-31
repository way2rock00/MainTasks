import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http';
import { UtilService } from 'src/app/shared/services/util.service';
import { Observable, of } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class SustainmentTabDataService {

  agileManager: any[] = [];
  selectedAgileManager: any[] = [];

  personas: any[] = [];
  selectedPersonas: any[] = [];

  deliverablesContentsJson: any[] = [];
  // deliverablesURL: string = `${environment.BASE_URL}/refineuserstories/deliverables/`;
  // deliverablesTabName: string = "Deliverables";
  // deliverablesStorage: string = "DELIVERABLESJSONBKP"

  userstoryLibraryContentsJson: any[] = [];
  // userstoryLibraryURL: string = `${environment.BASE_URL}/refineuserstories/userstorylibrary/`;
  // userStoryLibraryTabName: string = "User story library";
  // userStoryLibraryStorage: string = "USERSTORYLIBRARYJSONBKP";

  configWorkbooksContentsJson: any[] = [];
  // configWorkbooksURL: string = `${environment.BASE_URL}/refineuserstories/config/`;
  // configWorkbooksTabName: string = "Configuration workbooks";
  // configWorkbooksStorage: string = "CONFIGWORKBOOKSJSONBKP";

  constructor(private http: HttpClient, private utilService: UtilService) {
    this.clearData();
    this.clearFilters();
  }

  getTabDataURL(URL): Observable<any> { 
    return this.http.get<any>( `${environment.BASE_URL}${URL}${this.utilService.setfilterParamsURL()}`);
  }

  setSelectedFilter(e): Observable<any> {

    let self = this;
    if (this.utilService.isGlobalFilter(e.data.type)) {
      this.utilService.setSelectedFilter(e)
    }
    else {
      switch (e.data.type) {
        case "A":
          self.selectedAgileManager = [];
          self.selectedAgileManager = this.utilService.formL0Selection(e.data.selectedfilterData.l0);
          break;
        case "U":
          self.selectedPersonas = [];
          e.data.selectedfilterData.l0.map(function (p) {
            if (p.checked)
              self.selectedPersonas.push({ L0: p.L0 })
          });
          break;
      }

    }

    return of(e.data);
  }

  // filterTabData(): Observable<any> {

  //   this.clearData();
  //   this.getTabDataURL(this.configWorkbooksURL).subscribe(data => {
  //     this.configWorkbooksContentsJson = this.utilService.formTabContents(data, this.configWorkbooksTabName, this.configWorkbooksStorage);
  //   });

  //   this.formUserStoryLib();
  //   this.formDeliverables();

  //   return of(true);
  // }

  // formUserStoryLib(): Observable<any>{

  //   this.userstoryLibraryContentsJson = [];

  //   this.getTabDataURL(this.userstoryLibraryURL).subscribe(data => {
  //     this.userstoryLibraryContentsJson = this.utilService.formTabContents(data, this.userStoryLibraryTabName, this.userStoryLibraryStorage);
  //     this.personas = this.utilService.formadvancedFilter(this.userstoryLibraryContentsJson[0].tabContent, this.personas, "L2grp");
  //     this.userstoryLibraryContentsJson[0].tabContent = this.utilService.advancedFilters(this.userstoryLibraryContentsJson[0].tabContent, this.selectedPersonas, "L2grp");
  //   });

  //   return of(true)
  // }

  // formDeliverables(): Observable<any>{

  //   this.deliverablesContentsJson = [];
     
  //   this.getTabDataURL(this.deliverablesURL).subscribe(data => {
  //     this.deliverablesContentsJson = this.utilService.formTabContents(data, this.deliverablesTabName, this.deliverablesStorage);
  //     this.agileManager = this.utilService.formTechnologyFilter(this.deliverablesContentsJson[0].tabContent, this.agileManager, "L1value");
  //     this.deliverablesContentsJson[0].tabContent = this.utilService.technologyFilters(this.deliverablesContentsJson[0].tabContent, this.selectedAgileManager, "L1value");
  //   });

  //   return of(true)
  // }

  // updateDeliverablesJson(jsonDetails, projectId) {
  //   return this.http.post(this.deliverablesURL + projectId, jsonDetails);
  // }

  // updateUserstoryLibraryJson(jsonDetails, projectId) {
  //   return this.http.post(this.userstoryLibraryURL + projectId, jsonDetails);
  // }

  // updateConfigWorkbooksJson(jsonDetails, projectId) {
  //   return this.http.post(this.configWorkbooksURL + projectId, jsonDetails);
  // }

  clearData() {
    this.deliverablesContentsJson = [];
    this.userstoryLibraryContentsJson = [];
    this.configWorkbooksContentsJson = [];
  }

  clearFilters() {
    this.agileManager = [];
    this.selectedAgileManager = [];
    this.selectedPersonas = [];
    this.personas = [];
  }
}
