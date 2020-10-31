import { Component, OnInit } from '@angular/core';
import { SharedService } from 'src/app/shared/services/shared.service';
import { ValidateTabDataService } from 'src/app/feature/deliver/services/validate/validate-tab-data.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
//Filter changes
import { LAYOUT_TYPE, LAYOUT_DELIVER_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-test-scripts',
  templateUrl: './test-scripts.component.html',
  styleUrls: ['./test-scripts.component.scss']
})
export class TestScriptsComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.DELIVER;
  SUB_NAV: LAYOUT_DELIVER_SUB_NAV = LAYOUT_DELIVER_SUB_NAV.VALIDATE;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[0];
  toggledEvent: string;

  constructor(
    private globalData: PassGlobalInfoService,
    private sharedService: SharedService,
    private validateTabDataService: ValidateTabDataService,
    private messagingService: MessagingService,//Filter changes
    private utilService: UtilService,//Filter changes
    public dialog: MatDialog,//Filter changes
  ) { }

  ngOnInit() {

    this.validateTabDataService.clearData(); //Filter changes

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

    if (this.toggledEvent == "TESTSCRIPT TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.validateTabDataService.testScriptsContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL3TabCount('', this.validateTabDataService.testScriptsContentsJson[0].tabContent, 'L3grp', 'testenabledFlag','L2grp','testdoclink') });
  }
  
  setFilters() {
    this.validateTabDataService.testScriptsContentsJson = [];
    this.validateTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.validateTabDataService.testScriptsContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.emitTabCount();
    });
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, grandParent) {
    
    this.toggledEvent = "TESTSCRIPT TOGGLED"; //Filter changes

    // Flag toggle at L1 level
    if (j.L2grp != undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2grp) {
          if (i.L2enabledflag == "N") {
            i.L2enabledflag = "Y";
          }
          for (let k of i.L3grp) {
            if (k.testenabledFlag == "N") {
              k.testenabledFlag = "Y";
              this.sharedService.docAddEvent.emit(k.testenabledFlag);
            }
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.L2grp) {
          if (i.L2enabledflag == "Y") {
            i.L2enabledflag = "N";
          }
          for (let k of i.L3grp) {
            if (k.testenabledFlag == "Y") {
              k.testenabledFlag = "N";
              this.sharedService.docAddEvent.emit(k.testenabledFlag);
            }
          }
        }
      }
    }

    //Flag toggle at L2 level
    else if (grandParent == undefined) {
      if (j.L2enabledflag == "N") {
        j.L2enabledflag = "Y";
        if (parent.L2grp.find(t => t.L2enabledflag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
        }
        for (let i of j.L3grp) {
          if (i.testenabledFlag == "N") {
            i.testenabledFlag = "Y";
            this.sharedService.docAddEvent.emit(i.testenabledFlag);
          }
        }
      } else {
        j.L2enabledflag = "N";
        if (parent.L2grp.find(t => t.L2enabledflag == "N") == undefined) {
          parent.L1enabledflag = "Y";
        } else if (
          parent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
        ) {
          parent.L1enabledflag = "N";
        }
        for (let i of j.L3grp) {
          if (i.testenabledFlag == "Y") {
            i.testenabledFlag = "N";
            this.sharedService.docAddEvent.emit(i.testenabledFlag);
          }
        }
      }
    }
    //Flag toggle at L3 level
    else {
      if (j.testenabledFlag == "N") {
        j.testenabledFlag = "Y";
        this.sharedService.docAddEvent.emit(j.testenabledFlag);
        if (parent.L3grp.find(t => t.testenabledFlag == "Y") == undefined) {
          parent.L2enabledflag = "N";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "N") == undefined
          ) {
            grandParent.L1enabledflag = "Y";
          } else if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          }
        } else {
          parent.L2enabledflag = "Y";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          } else {
            grandParent.L1enabledflag = "Y";
          }
        }
      } else {
        j.testenabledFlag = "N";
        this.sharedService.docAddEvent.emit(j.testenabledFlag);
        if (parent.L3grp.find(t => t.testenabledFlag == "N") == undefined) {
          parent.L2enabledflag = "Y";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          } else {
            grandParent.L1enabledflag = "Y";
          }
        } else if (
          parent.L3grp.find(t => t.testenabledFlag == "Y") == undefined
        ) {
          parent.L2enabledflag = "N";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "N") == undefined
          ) {
            grandParent.L1enabledflag = "Y";
          } else if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          }
        }
      }
    }
    this.emitTabCount();
    event.stopPropagation();
  }
}
