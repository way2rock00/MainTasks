import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: 'root'
})
export class ValidateTabDataService {

  technology: any[] = [];
  selectedTechnology: any[] = [];

  testScriptsContentsJson: any[] = [];
  // testScriptsURL:string = `${environment.BASE_URL}/validate/test/`;
  // testScriptTabName: string = "Test scenarios and scripts";
  // testScriptStorage: string = "TESTSCRIPTJSONBKP";

  automationContentsJson: any[] = [];
  // automationURL:string = `${environment.BASE_URL}/validate/bots/`;
  // automationTabName: string = "Test automations";
  // automationStorage: string = "TESTAUTOMATIONJSONBKP";

  constructor(private http: HttpClient, private utilService: UtilService) { }

  getTabDataURL(URL): Observable<any> {
    return this.http.get<any>(`${environment.BASE_URL}${URL}${this.utilService.setfilterParamsURL()}`);
  }

  async setSelectedFilter(e): Promise<any> {

    var self = this;

    if (this.utilService.isGlobalFilter(e.data.type)) {
      this.utilService.setSelectedFilter(e)
    }
    else {
      switch (e.data.type) {
        case "P":
          self.selectedTechnology = [];
          e.data.selectedfilterData.l0.map(function (p) {
            if (p.checked)
              self.selectedTechnology.push({ L0: p.L0 })
          });
          break;
      }

    }

    return of(e.data);
  }

  // formAutomation(): Observable<any>{

  //   let advEleName = "technology";

  //   this.automationContentsJson = [];

  //   this.getTabDataURL(this.automationURL).subscribe(data => {
  //     this.automationContentsJson = this.utilService.formTabContents(data, this.automationTabName, this.automationStorage); 
  //     this.technology = this.utilService.formTechnologyFilter(this.automationContentsJson[0].tabContent,this.technology,advEleName);
  //     this.automationContentsJson[0].tabContent = this.utilService.technologyFilters(this.automationContentsJson[0].tabContent, this.selectedTechnology,advEleName);
  //   });

  //   return of(true);
  // }

  // async filterTabData(e): Promise<any> {

  //   this.clearData();

  //   this.getTabDataURL(this.testScriptsURL).subscribe(data => {
  //     this.testScriptsContentsJson = this.utilService.formTabContents(data, this.testScriptTabName, this.testScriptStorage); 
  //   });

  //   this.formAutomation();

  //   return true;
  // }

  clearData() {
    this.testScriptsContentsJson = [];
    this.automationContentsJson = [];
  }

  clearFilters() {
    this.technology = [];
    this.selectedTechnology = [];
  }

}
