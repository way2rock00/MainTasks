import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: 'root'
})
export class ConstructTabDataService {

  technology: any[] = [];
  selectedTechnology: any[] = [];

  conversionContentsJson: any[] = [];
  // conversionURL:string = `${environment.BASE_URL}/construct/conversion/`;
  // conversionStorage: string = "CONVERSIONJSONBKP";
  // conversionTabName: string = "Conversions"

  developmentToolContentsJson: any[] = [];
  // developmentToolURL: string = `${environment.BASE_URL}/construct/toolsv5/`;
  // developmentStorage: string = "DEVTOOLSJSONBKP";
  // developmentToolsTabName: string = "Development tools"

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

  // async filterTabData(e): Promise<any> {
  //   let advEleName = "technology";

  //   this.getTabDataURL(this.conversionURL).subscribe(data => {
  //     this.conversionContentsJson = this.utilService.formTabContents(data, this.conversionTabName, this.conversionStorage); 
  //     this.technology = this.utilService.formTechnologyFilter(this.conversionContentsJson[0].tabContent,this.technology,advEleName);
  //     this.conversionContentsJson[0].tabContent = this.utilService.technologyFilters(this.conversionContentsJson[0].tabContent, this.selectedTechnology, advEleName);
  //   });

  //   this.getTabDataURL(this.developmentToolURL).subscribe(data => {
  //     this.developmentToolContentsJson = this.utilService.formTabContents(data, this.developmentToolsTabName, this.developmentStorage); 
  //   });
  //   return true;
  // }

  clearData() {
    this.conversionContentsJson = [];
    this.developmentToolContentsJson = []

  }
  clearFilters() {
    this.technology = [];
    this.selectedTechnology = [];
  }

}
