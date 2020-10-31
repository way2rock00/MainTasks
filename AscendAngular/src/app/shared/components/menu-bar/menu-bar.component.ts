import { Component, Input, OnInit, ViewEncapsulation } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
import { SharedService } from 'src/app/shared/services/shared.service';
import { LAYOUT_TYPE } from '../../constants/layout-constants';
import { MessagingService } from '../../services/messaging.service';
import { MENU } from './../../constants/menu-bar-constant';
import { TAB_SAVE_CONST } from '../../constants/tab-change-save-dialog';

@Component({
  selector: 'app-menu-bar',
  templateUrl: './menu-bar.component.html',
  styleUrls: ['./menu-bar.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class MenuBarComponent implements OnInit {

  @Input() colorScheme: any;
  userId: string;
  homeUrl: string;
  projectName: String;
  clientName: String;
  view: String;
  imagineSelectedColor: string;
  deliverSelectedColor: string;
  runSelectedColor: string;
  insightsSelectedColor: string;
  projectGlobalInfo: ProjectGlobalInfoModel;
  imagineSolid: string;
  deliverSolid: string;
  runSolid: string;
  insightsSolid: string;
  imagineWeight: string;
  deliverWeight: string;
  runWeight: string;
  insightsWeight: string;
  trainStops = MENU;

  constructor(private router: Router, private activatedRoute: ActivatedRoute, private messagingBus: MessagingService, private globalData: PassGlobalInfoService,
    private sharedService: SharedService, private messagingService: MessagingService) {
    this.sharedService.selectedPageEvent.subscribe(data => {
      if (data == LAYOUT_TYPE.IMAGINE) {
        this.imagineSelectedColor = 'rgb(0, 151, 169)';
        this.imagineSolid = 'solid';
        this.imagineWeight = 'bold';
        this.deliverSelectedColor = '';
        this.deliverSolid = '';
        this.deliverWeight = '';
        this.runSelectedColor = '';
        this.runSolid = '';
        this.runWeight = '';
        this.insightsSelectedColor = '';
        this.insightsSolid = '';
        this.insightsWeight = '';
      } else if (data == LAYOUT_TYPE.DELIVER) {
        this.deliverSelectedColor = 'rgb(198, 215, 12)';
        this.deliverSolid = 'solid';
        this.deliverWeight = 'bold';
        this.imagineSolid = '';
        this.imagineWeight = '';
        this.imagineSelectedColor = '';
        this.runSelectedColor = '';
        this.runSolid = '';
        this.runWeight = '';
        this.insightsSelectedColor = '';
        this.insightsSolid = '';
        this.insightsWeight = '';
      } else if (data == LAYOUT_TYPE.RUN) {
        this.runSelectedColor = 'rgb(134, 188, 37)';
        this.runSolid = 'solid';
        this.runWeight = 'bold';
        this.imagineSolid = '';
        this.imagineWeight = '';
        this.imagineSelectedColor = '';
        this.deliverSelectedColor = '';
        this.deliverSolid = '';
        this.deliverWeight = '';
        this.insightsSelectedColor = '';
        this.insightsSolid = '';
        this.insightsWeight = '';
      } else if (data == LAYOUT_TYPE.INSIGHTS) {
        this.insightsSelectedColor = 'rgb(0, 85, 135)';
        this.insightsSolid = 'solid';
        this.insightsWeight = 'bold';
        this.runSelectedColor = '';
        this.runSolid = '';
        this.runWeight = '';
        this.imagineSolid = '';
        this.imagineWeight = '';
        this.imagineSelectedColor = '';
        this.deliverSelectedColor = '';
        this.deliverSolid = '';
        this.deliverWeight = '';
      }
    })
  }

  ngOnInit() {

    this.sharedService.dataChangeEvent.subscribe(data => {
      if (data.type == 2 && data.source == TAB_SAVE_CONST.MENU)
        this.router.navigate([`${data.data}`]);
    });
    this.globalData.share.subscribe(x => (this.projectGlobalInfo = x));
    this.activatedRoute.params.subscribe(route => {
      if (route.phaseName == LAYOUT_TYPE.IMAGINE) {
        this.imagineSelectedColor = 'rgb(0, 151, 169)';
        this.imagineSolid = 'solid';
        this.imagineWeight = 'bold';
        this.deliverSelectedColor = '';
        this.deliverSolid = '';
        this.deliverWeight = '';
        this.runSelectedColor = '';
        this.runSolid = '';
        this.runWeight = '';
        this.insightsSelectedColor = '';
        this.insightsSolid = '';
        this.insightsWeight = '';
      } else if (route.phaseName == LAYOUT_TYPE.DELIVER) {
        this.deliverSelectedColor = 'rgb(198, 215, 12)';
        this.deliverSolid = 'solid';
        this.deliverWeight = 'bold';
        this.imagineSolid = '';
        this.imagineWeight = '';
        this.imagineSelectedColor = '';
        this.runSelectedColor = '';
        this.runSolid = '';
        this.runWeight = '';
        this.insightsSelectedColor = '';
        this.insightsSolid = '';
        this.insightsWeight = '';
      } else if (route.phaseName == LAYOUT_TYPE.RUN) {
        this.runSelectedColor = 'rgb(134, 188, 37)';
        this.runSolid = 'solid';
        this.runWeight = 'bold';
        this.imagineSolid = '';
        this.imagineWeight = '';
        this.imagineSelectedColor = '';
        this.deliverSelectedColor = '';
        this.deliverSolid = '';
        this.deliverWeight = '';
        this.insightsSelectedColor = '';
        this.insightsSolid = '';
        this.insightsWeight = '';
      } else if (route.phaseName == LAYOUT_TYPE.INSIGHTS) {
        this.insightsSelectedColor = 'rgb(0, 85, 135)';
        this.insightsSolid = 'solid';
        this.insightsWeight = 'bold';
        this.runSelectedColor = '';
        this.runSolid = '';
        this.runWeight = '';
        this.imagineSolid = '';
        this.imagineWeight = '';
        this.imagineSelectedColor = '';
        this.deliverSelectedColor = '';
        this.deliverSolid = '';
        this.deliverWeight = '';
      }
    })
    this.projectName = this.projectGlobalInfo.projectName;
    this.clientName = this.projectGlobalInfo.clientName;
    this.view = this.projectGlobalInfo.viewMode;
    this.userId = this.messagingBus.getBus(BUS_MESSAGE_KEY.USER_DETAILS).value.userId;
    this.homeUrl = 'project/workspace/';

  }

  goToPage(menuItem) {
    if (this.sharedService.toggled == 'TOGGLED') {
      let dataChangeEventObj = {
        source: TAB_SAVE_CONST.MENU,
        data: menuItem.route,
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      this.router.navigate([`${menuItem.route}`]);
    }
  }

  goToHome(){
    if (this.sharedService.toggled == 'TOGGLED') {
      let dataChangeEventObj = {
        source: TAB_SAVE_CONST.MENU,
        data: '/home',
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      this.router.navigate([`/home`]);
    }
  }

}
