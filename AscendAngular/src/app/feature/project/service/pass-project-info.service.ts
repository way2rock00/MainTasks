import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs'

import { ProjectPassingInfoModel } from '../model/project-passing-info.model';

@Injectable()
export class PassProjectInfoService {
  private content = new BehaviorSubject<ProjectPassingInfoModel>( new ProjectPassingInfoModel());
  //private content = new BehaviorSubject<string>("Hello");
  public share = this.content.asObservable();
  constructor() { }

  updateData(projectPassingInfo){
    this.content.next(projectPassingInfo);
  }
}
