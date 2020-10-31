import { CryptUtilService } from './crypt-util.service';
import { EventEmitter, Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { ProjectGlobalInfoModel } from '../../shared/model/project-global-info.model';

@Injectable({
  providedIn: 'root'
})
export class PassGlobalInfoService {

  private content = new BehaviorSubject<ProjectGlobalInfoModel>(
    {
      userId: ""
      , projectId: ""
      , role: ""
      , viewMode: "EXPLORE"
      , uniqueId: ""
      , clientName: ""
      , projectName: ""
    });
  //private content = new BehaviorSubject<string>("Hello");
  public share = this.content.asObservable();

  constructor(private cryptoUtilService: CryptUtilService) { }

  updateGlobalData(projectPassingInfo) {
    this.cryptoUtilService.setItem('projectGlobalInfo', projectPassingInfo, 'LOCAL');
    this.content.next(projectPassingInfo);
  }
}
