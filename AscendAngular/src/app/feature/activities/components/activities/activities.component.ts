import { environment } from './../../../../../environments/environment';
import { Subscription } from 'rxjs';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { CryptUtilService } from './../../../../shared/services/crypt-util.service';
import { SharedService } from './../../../../shared/services/shared.service';
import { ActivitiesService } from './../../services/activities.service';

export interface ActivityNode {
  hierarchyId: number;
  entityName: string;
  entityType: string;
  entityId: number;
  entityDescription: string;
  parentHierarchyId: number;
  dataVisible: string;
  clickableContent: any[];
  level_value: number;
  children: ActivityNode[];
}

@Component({
  selector: 'app-activities',
  templateUrl: './activities.component.html',
  styleUrls: ['./activities.component.scss']
})
export class ActivitiesComponent implements OnInit {

  projectGlobalInfo: ProjectGlobalInfoModel;
  activitiesURL = environment.BASE_URL + "/activities/";
  filterData: any[] = [];
  functionPackageURL: any = '';
  functions: any[] = [];
  package: any[] = [];
  filteredTree: any[] = [];
  urlParams: any;
  subscription: Subscription;
  hierarchyDetEventValue: any;

  constructor(private cryptUtilService: CryptUtilService, private sharedService: SharedService, private route: ActivatedRoute, private activitiesService: ActivitiesService, private globalData: PassGlobalInfoService) {
  }

  ngOnInit() {
    this.route.params.subscribe(param => {
      this.urlParams = {
        stopName: param.stopName,
        phaseName: param.phaseName,
        routedFrom: param.route,
        activityId: param.activityId,
        tabCode: param.tabCode
      }
      this.filteredTree = this.filterData.length > 0 ? this.filterTree(this.activitiesService.filterConstruct(this.filterData), this.urlParams.stopName) : [];
    })
    this.subscription = this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
    });
    this.functionPackageURL = this.cryptUtilService.getItem(BUS_MESSAGE_KEY.IIDR_FILTER + "_functionpackage_" + this.projectGlobalInfo.projectId, 'SESSION');
    this.functions = this.functionPackageURL.slice(1).split('/')[1].split(',');
    this.package = this.functionPackageURL.slice(1).split('/')[0].split(',');
    this.sharedService.getData(this.activitiesURL + this.projectGlobalInfo.projectId + '/' + this.package[0] + '/' + this.functionPackageURL.slice(1).split('/')[1]).subscribe(y => {
      this.filterData = y;
      this.filteredTree = this.filterTree(this.activitiesService.filterConstruct(this.filterData), this.urlParams.stopName);
    })
  }

  filterTree(ultimateTree, stopName) {
    let filterChildren = [];
    for (let i of ultimateTree) {
      if (i.parentHierarchyId == null && i.entityName.toUpperCase() == stopName.toUpperCase()) {
        this.urlParams.trainStopDetails = i;
        for (let j of i.children) {
          filterChildren.push(j);
        }
        return filterChildren;
      }
    }
  }

  hierarchyDetEvent(event) {
    this.hierarchyDetEventValue = event;
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

}
