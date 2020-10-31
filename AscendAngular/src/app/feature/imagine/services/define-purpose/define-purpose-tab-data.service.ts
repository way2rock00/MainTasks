import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { UtilService } from 'src/app/shared/services/util.service';
import { Observable, of } from 'rxjs';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class DefinePurposeTabDataService {

  miscContentsJson: any[];
  // miscURL: string = `${environment.BASE_URL}/definedigitalorg/misc/`;
  // miscTabName: string = "Define digital organization";
  // miscStorage: string = "MISCJSONBKP";
  
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

  //   this.getTabDataURL(this.miscURL).subscribe(data => {
  //     this.miscContentsJson = this.utilService.formTabContents(data, this.miscTabName, this.miscStorage);
  //   });

  //   return of();
  // }

  clearData() {
    this.miscContentsJson = [];
  }
}
