import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { ConstructTabDataService } from 'src/app/feature/deliver/services/construct/construct-tab-data.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { SharedService } from 'src/app/shared/services/shared.service';
//Filter changes
import { LAYOUT_TYPE, LAYOUT_DELIVER_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-development-tools',
  templateUrl: './development-tools.component.html',
  styleUrls: ['./development-tools.component.scss']
})
export class DevelopmentToolsComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;

  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.DELIVER;
  SUB_NAV: LAYOUT_DELIVER_SUB_NAV = LAYOUT_DELIVER_SUB_NAV.CONSTRUCT;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[1];
  toggledEvent: string;

  constructor(private globalData: PassGlobalInfoService, private sharedService: SharedService,
    private constructTabDataService: ConstructTabDataService,
    private messagingService: MessagingService,//Filter changes
    private utilService: UtilService,//Filter changes
    public dialog: MatDialog,//Filter changes
  ) { }

  ngOnInit() {

    this.constructTabDataService.clearData(); //Filter changes

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

    if (this.toggledEvent == "DEVTOOLS TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.constructTabDataService.developmentToolContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL2TabCount('', this.constructTabDataService.developmentToolContentsJson[0].tabContent, 'toolgrp', 'toolenabledFlag','tooldoclink') });
  }

  setFilters() {
    this.constructTabDataService.developmentToolContentsJson = [];
    this.constructTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.constructTabDataService.developmentToolContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.emitTabCount();
    });
  }

  preview(doclink) {
    // window.location.href = doclink;
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent) {

    this.toggledEvent = "DEVTOOLS TOGGLED"; //Filter changes
    if (parent == undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.toolgrp) {
          if (i.toolenabledFlag == 'N') {
            i.toolenabledFlag = "Y";
            this.sharedService.docAddEvent.emit(i.toolenabledFlag);
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.toolgrp) {
          if (i.toolenabledFlag == 'Y') {
            i.toolenabledFlag = "N";
            this.sharedService.docAddEvent.emit(i.toolenabledFlag);
          }
        }
      }
    } else {
      if (j.toolenabledFlag == "N") {
        j.toolenabledFlag = "Y";
        this.sharedService.docAddEvent.emit(j.toolenabledFlag);
        if (parent.toolgrp.find(t => t.toolenabledFlag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
        }
      } else {
        j.toolenabledFlag = "N";
        this.sharedService.docAddEvent.emit(j.toolenabledFlag);
        if (parent.toolgrp.find(t => t.toolenabledFlag == "N") == undefined) {
          parent.L1enabledflag = "Y";
        } else if (
          parent.toolgrp.find(t => t.toolenabledFlag == "Y") == undefined
        ) {
          parent.L1enabledflag = "N";
        }
      }
    }
    this.emitTabCount();
    event.stopPropagation();
  }

}
