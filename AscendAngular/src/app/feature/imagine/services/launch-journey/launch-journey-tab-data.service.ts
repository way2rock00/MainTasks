import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { environment } from "src/environments/environment";
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
  providedIn: 'root'
})
export class LaunchJourneyTabDataService {

  constructor(private http: HttpClient, private utilService: UtilService) { }

  setSelectedFilter(e): Observable<any> {
    let self = this;

    if(this.utilService.isGlobalFilter(e.data.type)){
      this.utilService.setSelectedFilter(e)
    }

    return of(e.data);
  }
}
