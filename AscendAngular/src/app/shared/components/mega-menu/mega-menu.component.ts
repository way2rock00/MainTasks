import { CryptUtilService } from './../../services/crypt-util.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { Component, OnInit, ViewChild } from '@angular/core';
import { MatMenu } from '@angular/material';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';
import { MEGA_MENU } from './../../constants/mega-menu-options';
import { Router } from '@angular/router';
import { TAB_SAVE_CONST } from '../../constants/tab-change-save-dialog';

@Component({
  selector: 'app-mega-menu',
  templateUrl: './mega-menu.component.html',
  styleUrls: ['./mega-menu.component.scss'],
  exportAs: 'megaMenuComponent',
})

export class MegaMenuComponent implements OnInit {

  @ViewChild(MatMenu, { static: true }) megaMenu: MatMenu;
  megaMenuOptions: any = MEGA_MENU;
  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel = new ProjectGlobalInfoModel();
  uniqueId: any;
  enableExploreOption: any;
  self = this;

  constructor(private router: Router
    , private globalData: PassGlobalInfoService
    , private sharedService: SharedService
    , private cryptUtilService: CryptUtilService) { }

  ngOnInit() {
    this.globalData.share.subscribe(data => {
      this.view = data.viewMode;
      this.uniqueId = data.uniqueId;
    });

    this.sharedService.dataChangeEvent.subscribe(res => {
      if (res && res.type == 2 && res.source == TAB_SAVE_CONST.MEGA_MENU) {
        res.data.callback(this, res.data.route);
      }
    });
  }

  navigate(callback, route) {
    if (this.sharedService.toggled == 'TOGGLED') {
      let dataChangeEventObj = {
        source: TAB_SAVE_CONST.MEGA_MENU,
        data: {
          callback: callback,
          route: route
        },
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      callback(this, route);
    }
  }

  enterExploreMode(self, route) {
    self.projectGlobalInfo.viewMode = "EXPLORE";
    self.projectGlobalInfo.projectId = "0";
    self.projectGlobalInfo.uniqueId = "0";
    self.projectGlobalInfo.projectName = "";
    self.projectGlobalInfo.clientName = "";
    self.projectGlobalInfo.clientUrl = "";
    self.cryptUtilService.sessionClear();
    self.globalData.updateGlobalData(self.projectGlobalInfo);
    self.router.navigate([route]);
  }

  navigateSameTab(self, route) {
    self.router.navigate([route]);
  }

  navigateNewTab(self, route) {
    window.open(route);
  }
}
