import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { MarketplaceTools } from '../models/marketplace-tools.model';
import { MARKETPLACEFILTERCONST } from '../constants/marketplace-filter';

@Injectable({
  providedIn: 'root'
})
export class MarketplaceService {

  constructor(private http: HttpClient) { }

  getFilters(): Observable<any>{
    return this.http.get<any>(`${environment.BASE_URL}/marketplacefilter`);
    // return of(MARKETPLACEFILTERCONST);
  }

  getTools(): Observable<any>{
    return this.http.get<MarketplaceTools>(`${environment.BASE_URL}/marketplacetools`);
  }
}
