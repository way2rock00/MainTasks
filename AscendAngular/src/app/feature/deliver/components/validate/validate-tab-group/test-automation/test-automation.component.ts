import { Component, OnInit, Input } from "@angular/core";
import { SharedService } from "src/app/shared/services/shared.service";
import { ValidateTabDataService } from "src/app/feature/deliver/services/validate/validate-tab-data.service";
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
//Filter changes
import { LAYOUT_TYPE, LAYOUT_DELIVER_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: "app-test-automation",
  templateUrl: "./test-automation.component.html",
  styleUrls: ["./test-automation.component.scss"]
})
export class TestAutomationComponent implements OnInit {

  filters: any;
  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.DELIVER;
  SUB_NAV: LAYOUT_DELIVER_SUB_NAV = LAYOUT_DELIVER_SUB_NAV.VALIDATE;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[1];
  toggledEvent: string;
  bgColor: string = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.colorScheme;

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

    if (this.toggledEvent == "TESTAUTOMATION TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.validateTabDataService.automationContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL3TabCount('', this.validateTabDataService.automationContentsJson[0].tabContent, 'L3grp', 'botenabledFlag','L2grp','botdoclink') });
  }

  setFilters() {
    this.filters = [
      {
        filterValues: this.validateTabDataService.technology,
        selectedFilters: {
          L0: this.validateTabDataService.selectedTechnology,
          L1: []
        },
        type: "P",
        title: { main: "Technology", levels: [] },
        filterButtonTitle: "Technology"
      }
    ];

    let advEleName = "technology";

    this.validateTabDataService.automationContentsJson = [];

    this.validateTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.validateTabDataService.automationContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage); 
      this.validateTabDataService.technology = this.utilService.formTechnologyFilter(this.validateTabDataService.automationContentsJson[0].tabContent,this.validateTabDataService.technology,advEleName);
      this.validateTabDataService.automationContentsJson[0].tabContent = this.utilService.technologyFilters(this.validateTabDataService.automationContentsJson[0].tabContent, this.validateTabDataService.selectedTechnology,advEleName);
      this.emitTabCount();
    });
  }

  eventEmit(e) {
    var self = this;
    this.validateTabDataService.setSelectedFilter(e).then(function (data) {
      self.setFilters();
    });
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, grandParent) {

    this.toggledEvent = "TESTAUTOMATION TOGGLED"; //Filter changes
    // Flag toggle at L1 level
    if (j.L2grp != undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2grp) {
          if (i.L2enabledflag == "N") {
            i.L2enabledflag = "Y";
          }
          for (let k of i.L3grp) {
            if (k.botenabledFlag == "N") {
              k.botenabledFlag = "Y";
              this.sharedService.docAddEvent.emit(k.botenabledFlag);
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
            if (k.botenabledFlag == "Y") {
              k.botenabledFlag = "N";
              this.sharedService.docAddEvent.emit(k.botenabledFlag)
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
          if (i.botenabledFlag == "N") {
            i.botenabledFlag = "Y";
            this.sharedService.docAddEvent.emit(i.botenabledFlag);
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
          if (i.botenabledFlag == "Y") {
            i.botenabledFlag = "N";
            this.sharedService.docAddEvent.emit(i.botenabledFlag);
          }
        }
      }
    }
    //Flag toggle at L3 level
    else {
      if (j.botenabledFlag == "N") {
        j.botenabledFlag = "Y";
        this.sharedService.docAddEvent.emit(j.botenabledFlag);
        if (parent.L3grp.find(t => t.botenabledFlag == "Y") == undefined) {
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
        j.botenabledFlag = "N";
        this.sharedService.docAddEvent.emit(j.botenabledFlag);
        if (parent.L3grp.find(t => t.botenabledFlag == "N") == undefined) {
          parent.L2enabledflag = "Y";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          } else {
            grandParent.L1enabledflag = "Y";
          }
        } else if (
          parent.L3grp.find(t => t.botenabledFlag == "Y") == undefined
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

    this.emitTabCount()

    event.stopPropagation();
  }
}
