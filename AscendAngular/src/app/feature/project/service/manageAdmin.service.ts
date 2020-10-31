import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class ManageAdminService {

  readonly ADMINS = '/superUserList';

  constructor(private http : HttpClient) {}

  postData = {action: "CREATE"};

  getAdmins(): Observable<any> {
      const url = `${environment.BASE_URL}${this.ADMINS}`;
      return this.http.get(url)
      .pipe(
          map( (data: string[]) => {
              if (data && data.length) {
                return data;
              } else {
                  return [];
              }
          })
      );
  }

  createDeleteAdmin(postData): Observable<any> {
    const url = `${environment.BASE_URL}${this.ADMINS}`;
    //this.postData.action = action;
    return this.http.post(url, postData);
    
  }

}
