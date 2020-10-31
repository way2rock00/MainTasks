import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { UtilService } from 'src/app/shared/services/util.service';

@Injectable({
    providedIn: 'root'
})
export class ActivateTabDataService {

    activateContentsJson: any[];
    // activateURL: string = `${environment.BASE_URL}/activatedigitalorg/misc/`;
    // activateStorage: string = "ACTIVATEJSONBKP";
    // activateTabName: string = "Activate digital organization"

    constructor(private http: HttpClient, private utilService: UtilService) { }

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

    //     this.getTabDataURL(this.activateURL).subscribe(data => {
    //       this.activateContentsJson = this.utilService.formTabContents(data, this.activateTabName, this.activateStorage); 
    //     });
    //     return of();
    // }

    clearData() {
        this.activateContentsJson = [];
    }
}
