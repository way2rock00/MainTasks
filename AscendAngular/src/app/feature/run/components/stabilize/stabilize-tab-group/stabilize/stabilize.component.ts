import { Component, OnInit } from '@angular/core';
import { SharedService } from "src/app/shared/services/shared.service";
import { StabilizeTabDataService } from 'src/app/feature/run/services/stabilize/stabilize-tab-data.service';
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
//Filter changes
import { LAYOUT_TYPE, LAYOUT_RUN_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-stabilize',
  templateUrl: './stabilize.component.html',
  styleUrls: ['./stabilize.component.scss']
})
export class StabilizeComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;

  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.RUN;
  SUB_NAV: LAYOUT_RUN_SUB_NAV = LAYOUT_RUN_SUB_NAV.STABILIZE;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[0];
  toggledEvent: string;

  constructor(
    private globalData: PassGlobalInfoService,
    private sharedService: SharedService,
    private stabilizeTabDataService: StabilizeTabDataService,
    private messagingService: MessagingService,//Filter changes
    private utilService: UtilService,//Filter changes
    public dialog: MatDialog,//Filter changes
  ) { }

  ngOnInit() {
    this.stabilizeTabDataService.clearData(); //Filter changes

    //Filter changes
    this.messagingService.publish(BUS_MESSAGE_KEY.FILTER_CHANGED, undefined);

    //Filter changes
    this.filterSubscription = this.messagingService
      .subscribe(BUS_MESSAGE_KEY.FILTER_CHANGED, data => { if (data == this.tab.tabCode) this.setFilters() });

    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.view = this.projectGlobalInfo.viewMode;
    });
  }

  ngOnDestroy() {
    //Filter changes
    this.filterSubscription.unsubscribe();

    if (this.toggledEvent == "STABILIZE TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.stabilizeTabDataService.stabilizeContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL2TabCount('', this.stabilizeTabDataService.stabilizeContentsJson[0].tabContent, 'L2grp', 'L2enabledflag','L2doclink') });
  }

  setFilters(){
    this.stabilizeTabDataService.clearData();
    this.stabilizeTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.stabilizeTabDataService.stabilizeContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage); 
      this.emitTabCount();
    });
  }


  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, flag) {

    this.toggledEvent = "STABILIZE TOGGLED";

    if (parent == undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2grp) {
          if (i.L2enabledflag == 'N') {
            i.L2enabledflag = "Y";
            this.sharedService.docAddEvent.emit(i.L2enabledflag);
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.L2grp) {
          if (i.L2enabledflag == 'Y') {
            i.L2enabledflag = "N";
            this.sharedService.docAddEvent.emit(i.L2enabledflag);
          }
        }
      }
    } else {
      if (j.L2enabledflag == "N") {
        j.L2enabledflag = "Y";
        this.sharedService.docAddEvent.emit(j.L2enabledflag);
        if (parent.L2grp.find(t => t.L2enabledflag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
        }
      } else {
        j.L2enabledflag = "N";
        this.sharedService.docAddEvent.emit(j.L2enabledflag);
        if (parent.L2grp.find(t => t.L2enabledflag == "N") == undefined) {
          parent.L1enabledflag = "Y";
        } else if (
          parent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
        ) {
          parent.L1enabledflag = "N";
        }
      }
    }
    this.emitTabCount()
    event.stopPropagation();
  }
}
