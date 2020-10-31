import { Injectable } from '@angular/core';
import { Observable, of, from } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';
import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';

@Injectable({
  providedIn: 'root'
})
export class DigitalDesignTabDataService {

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.DESIGN;
  tabs : any[] = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs;

  interfaceFilter: any[] = [];
  selectedinterfaceFilter: any[] = [];

  personas: any[] = [];
  selectedPersonas: any[] = [];

  reportFilter: any[] = [];
  selectedreportFilter: any[] = [];

  // businessSolutionURL: string = `${environment.BASE_URL}${this.tabs[0].serviceURL}`;
  businessSolutionContentsJson: any[] = [];
  // businessSolutionTabName: string = this.tabs[0].tabName;
  // businessSolutionStorage: string = this.tabs[0].tabStorage;

  interfacesContentsJson: any[] = [];
  // interfacesURL: string = `${environment.BASE_URL}${this.tabs[1].serviceURL}`;
  // interfacesTabName: string = this.tabs[1].tabName;
  // interfacesStorage: string = this.tabs[1].tabStorage;

  reportsContentsJson: any[] = [];
  // reportsURL: string  =  `${environment.BASE_URL}${this.tabs[2].serviceURL}`;
  // reportsTabName: string = this.tabs[2].tabName;
  // reportsStorage: string = this.tabs[2].tabStorage;

  // leadingPracticeURL: string = `${environment.BASE_URL}/design/kbdv5/`;
  leadingPracticesContentsJson: any[] = [];
  // leadingPracticeTabName: string = "Key design decisions";
  // leadingPracticeStorage: string = "KBDJSONBKP";

  userStoriesContentsJson: any[] = [];
  // userStoriesURL: string = `${environment.BASE_URL}/design/userstoriesv5/`;
  // userStoriesTabName: string = "User stories";
  // userStoriesStorage: string = "USERSTORIESJSONBKP";

  businessProcessContentsJson: any[] = [];
  // businessProcessURL: string = `${environment.BASE_URL}${this.tabs[5].serviceURL}`;
  // businessProcessTabName: string = this.tabs[5].tabName;
  // businessProcessStorage: string = this.tabs[5].tabStorage;

  configContentsJson: any[] = [];
  // configURL:string = `${environment.BASE_URL}/design/configurations/`;
  // configTabName: string = "ERP configurations";
  // configStorage: string = "CONFIGURATIONJSONBKP";

  constructor(private http: HttpClient, private utilService: UtilService) { }

  getTabDataURL(URL): Observable<any> { 
    return this.http.get<any>( `${environment.BASE_URL}${URL}${this.utilService.setfilterParamsURL()}`);
  }

  async setSelectedFilter(e): Promise<any> {

    var self = this;

    if(this.utilService.isGlobalFilter(e.data.type)){
      this.utilService.setSelectedFilter(e)
    }
    else{

      switch (e.data.type) {
        case "P":
          self.selectedinterfaceFilter = [];
          e.data.selectedfilterData.l0.map(function (p) {
            if (p.checked)
              self.selectedinterfaceFilter.push({ L0: p.L0 })
          });
          break;
  
        case "R":
          self.selectedreportFilter = [];
          e.data.selectedfilterData.l0.map(function (p) {
            if (p.checked)
              self.selectedreportFilter.push({ L0: p.L0 })
          });
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

  // async filterTabData(): Promise<any> {
  //   this.clearData();

  //   this.getTabDataURL(this.businessSolutionURL).subscribe(data => {
  //     this.businessSolutionContentsJson = this.utilService.formTabContents(data, this.businessSolutionTabName, this.businessSolutionStorage);    
  //   });

  //   this.formInterfaces();
  //   this.formReports();
  //   this.formUserStories();

  //   this.getTabDataURL(this.leadingPracticeURL).subscribe(data => {
  //     this.leadingPracticesContentsJson = this.utilService.formTabContents(data, this.leadingPracticeTabName, this.leadingPracticeStorage);    
  //   });

  //   this.getTabDataURL(this.businessProcessURL).subscribe(data => {
  //     this.businessProcessContentsJson = this.utilService.formTabContents(data, this.businessProcessTabName, this.businessProcessStorage);    
  //   });

    

  //   this.getTabDataURL(this.configURL).subscribe(data => {
  //     this.configContentsJson = this.utilService.formTabContents(data, this.configTabName, this.configStorage); 
  //   });

  //   return true;
  // }

  // formReports(): Observable<any>{
  //   let advEleName = "technology";

  //   this.reportsContentsJson = [];

  //   this.getTabDataURL(this.reportsURL).subscribe(data => {
  //     this.reportsContentsJson = this.utilService.formTabContents(data, this.reportsTabName, this.reportsStorage); 
  //     this.reportFilter = this.utilService.formTechnologyFilter(this.reportsContentsJson[0].tabContent,this.reportFilter, advEleName);
  //     this.reportsContentsJson[0].tabContent = this.utilService.technologyFilters(this.reportsContentsJson[0].tabContent, this.selectedreportFilter,advEleName);
  //   });

  //   return of(true);
  // }

  // formInterfaces(): Observable<any>{
  //   let advEleName = "technology";

  //   this.interfacesContentsJson = [];

  //   this.getTabDataURL(this.interfacesURL).subscribe(data => {
  //     this.interfacesContentsJson = this.utilService.formTabContents(data, this.interfacesTabName, this.interfacesStorage); 
  //     this.interfaceFilter = this.utilService.formTechnologyFilter(this.interfacesContentsJson[0].tabContent,this.interfaceFilter,advEleName);
  //     this.interfacesContentsJson[0].tabContent = this.utilService.technologyFilters(this.interfacesContentsJson[0].tabContent, this.selectedinterfaceFilter,advEleName);
  //   });
  //   return of(true);
  // }

  // formUserStories(): Observable<any>{

  //   this.userStoriesContentsJson = [];

  //   this.getTabDataURL(this.userStoriesURL).subscribe(data => {
  //     this.userStoriesContentsJson = this.utilService.formTabContents(data, this.userStoriesTabName, this.userStoriesStorage);  
  //     this.personas = this.utilService.formadvancedFilter(this.userStoriesContentsJson[0].tabContent, this.personas, "L2grp");
  //     this.userStoriesContentsJson[0].tabContent = this.utilService.advancedFilters(this.userStoriesContentsJson[0].tabContent,this.selectedPersonas, "L2grp");
  //   });
  //   return of(true);
  // }


  clearData(){
    this.businessSolutionContentsJson = [];
    this.interfacesContentsJson = [];
    this.reportsContentsJson = [];
    this.leadingPracticesContentsJson = [];
    this.userStoriesContentsJson = [];
    this.businessProcessContentsJson = [];
    this.configContentsJson = [];
  }

  clearFilters(){
    this.selectedPersonas = [];
    this.selectedreportFilter = [];
    this.selectedinterfaceFilter = [];
    this.personas = [];
    this.reportFilter = [];
    this.interfaceFilter = [];
  }

}
