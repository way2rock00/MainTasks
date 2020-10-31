import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class ToolsBarService {

  private toolsURL : string = `${environment.BASE_URL}/toolbar`

  constructor(private http: HttpClient) { }

  getToolsDataURL(): Observable<any> {
    return this.http.get<any>( this.toolsURL );
  }
}
