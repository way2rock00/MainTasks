import { Component, Input, OnInit, ViewEncapsulation } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { LAYOUT_CONFIGURATION, LAYOUT_TYPE } from 'src/app/shared/constants/layout-constants';
import { TAB_SAVE_CONST } from 'src/app/shared/constants/tab-change-save-dialog';
import { SharedService } from 'src/app/shared/services/shared.service';
import { ActivitiesService } from '../../services/activities.service';
import { PROJECT_SUMMARY_MOMENTUM_CONST } from '../../../project/constants/project-summary-momentum';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';

@Component({
  selector: 'app-activities-details',
  templateUrl: './activities-details.component.html',
  styleUrls: ['./activities-details.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class ActivitiesDetailsComponent implements OnInit {

  activitiesView: boolean = true;
  buttonDetails: any[] = [];
  subscription: Subscription;
  @Input() functionPackageURL: any;
  @Input() urlParams: any;
  @Input() hierarchyDet: any;
  @Input() projectName : any; //Demo change 8/18
  currentTabCode: string;
  activity: any;
  textColorScheme: any;

  LAYOUT: LAYOUT_TYPE;
  SUB_NAV: any;
  colorScheme: any;

  projectGlobalInfo: ProjectGlobalInfoModel;

  menuItems: any[] = [
    {
      label: 'Process Scope Generator',
      route: '/project/list'
    }
  ]

  alpMenuItems: any[] = [
    {
      label: 'Workshop Navigator',
      route: '/project/wsnprojectlist'
    }
  ]

  constructor(private router: Router, private sharedService: SharedService, private globalData: PassGlobalInfoService) {
  }

  ngOnInit() {
    this.subscription = this.sharedService.dataChangeEvent.subscribe(data => {
      if (data.type == 2 && data.source == TAB_SAVE_CONST.ACTIVITY_BACK)
        this.activitiesView = true;
    })

    this.subscription = this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
    });
  }

  ngOnChanges() {
    this.LAYOUT = this.urlParams.phaseName;
    this.SUB_NAV = this.urlParams.stopName;
    this.colorScheme = LAYOUT_CONFIGURATION[this.LAYOUT.toLowerCase()][this.SUB_NAV.toLowerCase()].right.colorScheme;
    this.textColorScheme = LAYOUT_CONFIGURATION[this.LAYOUT.toLowerCase()][this.SUB_NAV.toLowerCase()].right.textColorScheme;
    this.activitiesView = true;
    if (this.hierarchyDet) {
      for (let i of this.hierarchyDet) {
        i.enableBtnDiv = false;
        for (let j of i.clickableContent) {
          if (j.buttonName == 'View deliverables' && (j.buttonDetails && j.buttonDetails.length > 0)) {
            i.enableBtnDiv = true;
          } else if (j.buttonName == 'View amplifiers' && (j.buttonDetails && (j.buttonDetails[0].filterId != null && j.buttonDetails[0].filterId.length > 0 && j.buttonDetails.length > 0))) {
            i.enableBtnDiv = true;
          }
        }
      }
      if (this.urlParams.routedFrom.toUpperCase() != 'IIDR') {
        for (let j of this.hierarchyDet) {
          if (this.urlParams.activityId == j.entityId) {
            for (let i of j.clickableContent) {
              if (i.buttonName.toUpperCase() == 'VIEW DELIVERABLES')
                this.switchView(i, j)
            }
          }
        }
      }
    }
  }

  goToPage(menuItem) {
    this.router.navigate(['/project/psg/' +this.projectGlobalInfo.projectId]);
    //this.router.navigate([menuItem.route]);
  }

  switchView(clickableContent, activity) {
    if (clickableContent.buttonName.toUpperCase() == 'VIEW DELIVERABLES') {
      this.buttonDetails = [];
      this.buttonDetails = clickableContent.buttonDetails;
      this.activity = activity;
      this.activitiesView = false;
    } else {
      this.router.navigate(['/marketplace/' + clickableContent.buttonDetails[0].filterId.toString() + '/' + clickableContent.buttonDetails[0].toolId.toString()])
    }
  }

  //Demo change 8/18
  goToLink(stopName){
    let ceObject = PROJECT_SUMMARY_MOMENTUM_CONST.find( t => t.projectName.toLowerCase() == this.projectName.toLowerCase());
    let ceLink : any = ceObject ? (ceObject.momentum as any).find( t => t.artifactType == 'CapabilityEdge') : null;
    if(stopName == "Create digital ambitions" && ceLink && ceLink.artifactImg)
    window.open('https://capabilityedge.deloitte.com/map/6574/cluster-level')
  }

  back() {
    if (this.sharedService.toggled.toUpperCase() == 'TOGGLED') {
      let dataChangeEventObj = {
        source: TAB_SAVE_CONST.ACTIVITY_BACK,
        data: null,
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      this.activitiesView = true;
    }
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

}
