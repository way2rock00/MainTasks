import { Component, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from "rxjs/Subscription";
import { User } from 'src/app/feature/project/constants/ascend-user-info';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { environment } from 'src/environments/environment';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
//Changes for SSO Integration.
import { MessagingService } from '../../services/messaging.service';
import { CryptUtilService } from '../../services/crypt-util.service';
import { TAB_SAVE_CONST } from '../../constants/tab-change-save-dialog';

@Component({
  selector: 'app-navigation-bar',
  templateUrl: './navigation-bar.component.html',
  styleUrls: ['./navigation-bar.component.scss']
})
export class NavigationBarComponent implements OnInit, OnDestroy {

  //@Input() title: any;
  inputBoxVisible: boolean;
  view;
  private subscription: Subscription;
  userData: User;
  url = "https://graph.microsoft.com/v1.0/me";
  docsCount: number;
  docsCountTemp: number;
  projectGlobalInfo: ProjectGlobalInfoModel;
  summaryScreen: any;
  decPassword: string;
  lsProjectGlobalData: any = this.cryptoUtilService.getItem('projectGlobalInfo', 'LOCAL');

  constructor(
    private sharedService: SharedService,
    private router: Router,
    private globalData: PassGlobalInfoService,
    private messagingBus: MessagingService,
    private cryptoUtilService: CryptUtilService
  ) {
    this.globalData.share.subscribe(x => this.projectGlobalInfo = x);
  }

  ngOnInit() {

    if (this.lsProjectGlobalData) {
      this.globalData.updateGlobalData(this.lsProjectGlobalData);
    }

    this.subscription = this.messagingBus
      .subscribe(BUS_MESSAGE_KEY.USER_DETAILS, (data) => {
        this.userData = data;
      });

    this.globalData.share.subscribe(data => {
      this.view = data.viewMode;
      // if (data.viewMode == 'PROJECT') {
      //   this.docsCount = 0;
      //   this.docsCountTemp = 0;
      //   this.sharedService.getData(`${environment.BASE_URL}/project/summary/${data.projectId}`).subscribe(data => {
      //     for (let i of data[0].artifacts) {
      //       this.docsCount += i.artifactCount;
      //     }
      //     this.docsCountTemp = this.docsCount;
      //   })
      // }
    });

    //set package function filter selected flag
    let packageFunction = this.cryptoUtilService.getItem(BUS_MESSAGE_KEY.IIDR_FILTER + "_functionpackage_" + this.projectGlobalInfo.projectId, 'SESSION');
    if (packageFunction)
      this.sharedService.filterSelected = true;
    else
      this.sharedService.filterSelected = false;

    this.sharedService.docAddEvent.subscribe(data => {
      if (data != undefined || data != '') {
        if (data == 'Y') {
          this.docsCountTemp++;
        } else if (data == 'N') {
          this.docsCountTemp--;
        } else if (data == 'RESET' || data== 'FAILED') {
          this.docsCountTemp = this.docsCount;
        } else if (data == 'UPDATE') {
          this.docsCount = this.docsCountTemp;
        }
      }
    });

    //Tab post change
    this.sharedService.dataChangeEvent.subscribe(data => {
      if (data && data.type == 2 && data.source == TAB_SAVE_CONST.NAVIGATION_BAR)
        this.router.navigate([`${data.data}`]);
    });

    // this.sharedService.getTabs().subscribe(data => {
    //   this.sharedService.getIndex(data,this.router.url);
    // });
  }

  searchClicked() {
    this.inputBoxVisible = !this.inputBoxVisible
  }

  emitEvent(identifier, route) {
    if (this.sharedService.toggled == 'TOGGLED') {
      let dataChangeEventObj = {
        source: identifier,
        data: route,
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      this.router.navigate([route]);
    }

  }

  navigateToSummary(projectId, mode, userId) {
    if (this.view != 'EXPLORE' && this.sharedService.filterSelected) {
      this.emitEvent(TAB_SAVE_CONST.NAVIGATION_BAR, '/project/summary')
    }
  }

  goToHome() {
    this.emitEvent(TAB_SAVE_CONST.NAVIGATION_BAR, '/project/workspace');
  }

  ngOnDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }
  }
}
