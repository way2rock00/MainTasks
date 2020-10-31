import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { MarketplaceSolutionTools } from '../models/marketplacesolutions-tools.model';
import { MARKETPLACESOLUTIONSFILTERCONST } from '../constants/marketplace-solutions-filter';

@Injectable({
  providedIn: 'root'
})
export class MarketplaceSolutionsService {

  constructor(private http: HttpClient) { }

  getFilters(): Observable<any>{
    return this.http.get<any>(`${environment.BASE_URL}/marketplaceSolutionsFilters`);
    //return of(MARKETPLACESOLUTIONSFILTERCONST);
  }

  getTools(): Observable<any>{
    return this.http.get<MarketplaceSolutionTools>(`${environment.BASE_URL}/marketplaceSolutions`);
  }
}
