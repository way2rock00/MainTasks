import {Observable} from 'rxjs/Rx'
import {HttpClient, HttpParams} from '@angular/common/http';
import {Injectable} from '@angular/core';

@Injectable()
export class HttpServiceHelper {

  constructor(private http: HttpClient) {
  }

  public httpGetRequestWithParams(url: string, param: HttpParams) {
    return this.http.get(url, { params: param })
      .map(response => {
        return response;
      })
      .catch(response => (Observable.throw(response)
      ));
  }

  public httpGetRequest(url: string) {
    return this.http.get(url)
      .map(response => {
        return response;
      })
      .catch(response => (Observable.throw(response)
      ))
  }

}
