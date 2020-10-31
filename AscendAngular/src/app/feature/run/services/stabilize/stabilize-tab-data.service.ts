import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: 'root'
})
export class StabilizeTabDataService {

  stabilizeContentsJson: any[];
  // stabilizeURL: string = `${environment.BASE_URL}/stablize/misc/`;
  // stabilizeTabName: string = "Stabilize";
  // stabilizeStorage: string = "STABILIZEJSONBKP";

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

  //   this.getTabDataURL(this.stabilizeURL).subscribe(data => {
  //     this.stabilizeContentsJson = this.utilService.formTabContents(data, this.stabilizeTabName, this.stabilizeStorage); 
  //   });

  //   return of();
  // }

  clearData() {
    this.stabilizeContentsJson = [];
  }
}
