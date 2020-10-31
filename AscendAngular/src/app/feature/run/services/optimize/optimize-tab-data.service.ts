import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: 'root'
})
export class OptimizeTabDataService {

  regressionTestContentsJson: any[];
  // regressionTestURL: string = `${environment.BASE_URL}/optimize/regressiontest/`;
  // regressionTestTabName: string = "Regression testing";
  // regressionStorage: string = "REGRESSIONTESTJSONBKP"

  aceContentsJson: any[];
  // acetURL: string = `${environment.BASE_URL}/optimize/quarterlyinsights/`;
  // aceTabName: string = "ACE quarterly release insights";
  // aceStorage: string = "ACEJSONBKP";

  constructor(private http: HttpClient, private utilService: UtilService) {
    this.clearData();
  }

  getTabDataURL(URL): Observable<any> { 
    return this.http.get<any>( `${environment.BASE_URL}${URL}${this.utilService.setfilterParamsURL()}`);
  }

  setSelectedFilter(e): Observable<any> {

    this.clearData();

    if (this.utilService.isGlobalFilter(e.data.type)) {
      this.utilService.setSelectedFilter(e)
    }

    return of(e.data);
  }

  // filterTabData(): Observable<any> {
  //   this.getTabDataURL(this.regressionTestURL).subscribe(data => {
  //     this.regressionTestContentsJson = this.utilService.formTabContents(data, this.regressionTestTabName, this.regressionStorage); 
  //   });

  //   this.getTabDataURL(this.acetURL).subscribe(data => {
  //     this.aceContentsJson = this.utilService.formTabContents(data, this.aceTabName, this.aceStorage); 
  //   });

  //   return of();
  // }

  clearData() {
    this.regressionTestContentsJson = [];
    this.aceContentsJson = [];
  }
}
