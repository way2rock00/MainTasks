import { Component, OnInit, ViewEncapsulation, Input } from "@angular/core";
import { SharedService } from "src/app/shared/services/shared.service";
import { DigitalDesignTabDataService } from "src/app/feature/imagine/services/digital-design/digital-design-tab-data.service";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
//Filter changes
import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: "app-reports",
  templateUrl: "./reports.component.html",
  styleUrls: ["./reports.component.scss"],
  encapsulation: ViewEncapsulation.None
})
export class ReportsComponent implements OnInit {

  filters: any;
  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.DESIGN;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[2];
  toggledEvent: string;
  bgColor: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.colorScheme;

  constructor(
    private globalData: PassGlobalInfoService,
    private sharedService: SharedService,
    private digitalDesignTabDataService: DigitalDesignTabDataService,
    private messagingService: MessagingService,//Filter changes
    private utilService: UtilService,//Filter changes
    public dialog: MatDialog,//Filter changes
  ) { }

  ngOnInit() {

    this.digitalDesignTabDataService.clearData(); //Filter changes
    this.digitalDesignTabDataService.clearFilters(); //Filter changes

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

    if (this.toggledEvent == "REPORTS TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.digitalDesignTabDataService.reportsContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL3TabCount('', this.digitalDesignTabDataService.reportsContentsJson[0].tabContent, 'L3grp', 'reportenabledFlag', 'L2grp','reportdoclink')});
  }

  setFilters() {
    this.filters = [
      {
        filterValues: this.digitalDesignTabDataService.reportFilter,
        selectedFilters: {
          L0: this.digitalDesignTabDataService.selectedreportFilter,
          L1: []
        },
        type: "R",
        title: { main: "Technology", levels: [] },
        filterButtonTitle: "Technology"
      }
    ];

    let advEleName = "technology";

    this.digitalDesignTabDataService.reportsContentsJson = [];

    this.digitalDesignTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.digitalDesignTabDataService.reportsContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage); 
      this.digitalDesignTabDataService.reportFilter = this.utilService.formTechnologyFilter(this.digitalDesignTabDataService.reportsContentsJson[0].tabContent,this.digitalDesignTabDataService.reportFilter, advEleName);
      this.digitalDesignTabDataService.reportsContentsJson[0].tabContent = this.utilService.technologyFilters(this.digitalDesignTabDataService.reportsContentsJson[0].tabContent, this.digitalDesignTabDataService.selectedreportFilter,advEleName);
      this.emitTabCount();
    });
  }

  eventEmit(e) {
    var self = this;
    this.digitalDesignTabDataService.setSelectedFilter(e).then(function (data) {
      self.setFilters();
    });
  }

  preview(doclink) {
    // window.location.href = doclink;
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, grandParent) {
    
    this.toggledEvent = "REPORTS TOGGLED"
    
    if (parent == undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2grp) {
          i.L2enabledflag = "Y";
          for (let k of i.L3grp) {
            if (k.reportenabledFlag == 'N') {
              k.reportenabledFlag = "Y";
              this.sharedService.docAddEvent.emit(k.reportenabledFlag);
            }
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.L2grp) {
          i.L2enabledflag = "N";
          for (let k of i.L3grp) {
            if (k.reportenabledFlag == "Y") {
              k.reportenabledFlag = "N";
              this.sharedService.docAddEvent.emit(k.reportenabledFlag);
            }
          }
        }
      }
    } else if (grandParent == undefined) {
      if (j.L2enabledflag == "N") {
        j.L2enabledflag = "Y";
        if (parent.L2grp.find(t => t.L2enabledflag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
        }
        for (let k of j.L3grp) {
          if (k.reportenabledFlag == "N") {
            k.reportenabledFlag = "Y";
            this.sharedService.docAddEvent.emit(k.reportenabledFlag);
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
        for (let k of j.L3grp) {
          if (k.reportenabledFlag == "Y") {
            k.reportenabledFlag = "N";
            this.sharedService.docAddEvent.emit(k.reportenabledFlag);
          }
        }
      }
    } else {
      if (j.reportenabledFlag == "Y") {
        j.reportenabledFlag = "N";
        this.sharedService.docAddEvent.emit(j.reportenabledFlag);
        if (parent.L3grp.find(t => t.reportenabledFlag == "N") == undefined) {
          parent.L2enabledflag = "Y";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          } else {
            grandParent.L1enabledflag = "Y";
          }
        } else if (
          parent.L3grp.find(t => t.reportenabledFlag == "Y") == undefined
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
      } else {
        j.reportenabledFlag = "Y";
        this.sharedService.docAddEvent.emit(j.reportenabledFlag);
        if (parent.L3grp.find(t => t.reportenabledFlag == "Y") == undefined) {
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
      }
    }

    this.emitTabCount()
    event.stopPropagation();
  }
}
