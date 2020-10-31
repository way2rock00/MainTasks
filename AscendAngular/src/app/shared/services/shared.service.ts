import { Injectable, EventEmitter } from '@angular/core';
import { Observable, of, BehaviorSubject } from 'rxjs';
import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class SharedService {

  private searchLinkURL: string = `${environment.BASE_URL}/searchlinks`;
  private currentTabIndex = new BehaviorSubject<number>(0);
  public tabIndexShare = this.currentTabIndex.asObservable();

  private bgColor = new BehaviorSubject<string>("");
  public bgColorShare = this.bgColor.asObservable();

  public tabCountEvent = new EventEmitter();

  public dataChangeEvent = new EventEmitter();

  public toggled: string = '';

  filterSelected: boolean = false;

  publishBgColor(color) {
    this.bgColor.next(color);
  }

  public docAddEvent = new EventEmitter();

  public toggledEvent = new EventEmitter();

  public selectedPageEvent = new EventEmitter();

  public selectedTabEvent = new EventEmitter();

  public summaryFilterEvent = new EventEmitter();

  options: any[] = [
    {
      value: 'Leading Practices',
      route: '/imagine/architect/Leading Practices',
      tab: 'Leading Practices',
      L0: '',
      L1: '',
      L2: '',
      industry: '',
      sector: '',
      region: '',
      persona: ''
    },
    {
      value: 'Userstories',
      route: '/imagine/architect/Userstories',
      tab: 'Userstories',
      L0: '',
      L1: '',
      L2: '',
      industry: '',
      sector: '',
      region: '',
      persona: ''
    },
    {
      value: 'Business Processes',
      route: '/imagine/architect/Business Processes',
      tab: 'Business Processes',
      L0: '',
      L1: '',
      L2: '',
      industry: '',
      sector: '',
      region: '',
      persona: ''
    },
    {
      value: 'ERP Configurations',
      route: '/imagine/architect/ERP Configurations',
      tab: 'ERP Configurations',
      L0: '',
      L1: '',
      L2: '',
      industry: '',
      sector: '',
      region: '',
      persona: ''
    }
  ];

  constructor(private http: HttpClient, private router: Router) { }

  getTabs(): Observable<any[]> {
    //  return of(this.options);
    return this.http.get<any>(this.searchLinkURL);
  }

  getIndex(tabs, URL) {
    URL = decodeURI(URL);
    URL = URL.charAt(0) == '/' ? URL.substr(1) : URL
    let currentTab = tabs.find(t => t.route == URL)
    if (currentTab) {
      this.currentTabIndex.next(currentTab.tab);
    }
  }

  getData(URL): Observable<any[]> {
    return this.http.get<any>(URL);
  }

  postData(URL, body): Observable<any> {
    return this.http.post<any>(URL, body);
  }
}
