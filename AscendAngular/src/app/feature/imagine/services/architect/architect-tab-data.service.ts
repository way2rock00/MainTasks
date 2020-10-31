import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { environment } from "src/environments/environment";
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: "root"
})
export class ArchitectTabDataService {

  // LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  // SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.ARCHITECT;
  // tabs : any[] = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs;
  
  personas: any[] = [];
  selectedPersonas: any[] = [];

  journeyMaps: any[] = [];
  selectedjourneyMaps: any[] = [];

  personasContentsJson: any[] = [];
  // personasURL: string = `${environment.BASE_URL}${this.tabs[0].serviceURL}`;
  // personaTabName: string = this.tabs[0].tabName;
  // personaStorage: string = this.tabs[0].tabStorage;

  journeyMapsContentsJson: any[] = [];
  // journeyMapsURL: string = `${environment.BASE_URL}${this.tabs[1].serviceURL}`;
  // journeyMapTabName: string = this.tabs[1].tabName;
  // journeyMapStorage: string = this.tabs[1].tabStorage;

  constructor(private http: HttpClient, private utilService: UtilService) {
    this.clearData();
    this.clearFilters();
   }

  getTabDataURL(URL): Observable<any> { 
    return this.http.get<any>( `${environment.BASE_URL}${URL}${this.utilService.setfilterParamsURL()}`);
  }

  // filterTabData(): Observable<any> {

  //   this.clearData();
  //   this.updatePersonas();
  //   this.updateJourneyMaps();
  //   return of(true);
  // }

  // updateJourneyMaps(): Observable<any> {
  //   this.journeyMapsContentsJson = [];
  //   this.getTabDataURL(this.journeyMapsURL).subscribe(data => {
  //     this.journeyMapsContentsJson = this.utilService.formTabContents(data, this.journeyMapTabName, this.journeyMapStorage);
  //     this.journeyMaps = this.utilService.formadvancedFilter( this.journeyMapsContentsJson[0].tabContent, this.journeyMaps, "L2Grp"  );
  //     this.journeyMapsContentsJson[0].tabContent = this.utilService.advancedFilters(this.journeyMapsContentsJson[0].tabContent,this.selectedjourneyMaps, "L2Grp" )
  //   });
  //   return of(true);
  // }

  // updatePersonas(): Observable<any> {
  //   this.personasContentsJson = [];
  //   this.getTabDataURL(this.personasURL).subscribe(data => {
  //     this.personasContentsJson = this.utilService.formTabContents(data, this.personaTabName, this.personaStorage);
  //     this.personas = this.utilService.formadvancedFilter( this.personasContentsJson[0].tabContent, this.personas, "L2Grp" );
  //     this.personasContentsJson[0].tabContent = this.utilService.advancedFilters(this.personasContentsJson[0].tabContent,this.selectedPersonas, "L2Grp" );
  //   });
  //   return of(true);
  // }

  async setSelectedFilter(e): Promise<any> {
    let self = this;

    if(this.utilService.isGlobalFilter(e.data.type)){
      this.utilService.setSelectedFilter(e)
    }
    else{

      switch (e.data.type) {
        case "P":
          self.selectedPersonas = [];
          self.selectedPersonas = self.utilService.formL0Selection(
            e.data.selectedfilterData.l0
          );
          break;
        case "J":
            self.selectedjourneyMaps = [];
            self.selectedjourneyMaps = self.utilService.formL0Selection(
              e.data.selectedfilterData.l0
            );
            break;
      }
    }
    return of(e.data);
  }

  clearData(){
    this.personasContentsJson = [];
    this.journeyMapsContentsJson = [];
  }
  clearFilters(){
    this.selectedPersonas = [];
    this.selectedjourneyMaps = [];
    this.personas = [];
    this.journeyMaps = [];
  }
}
