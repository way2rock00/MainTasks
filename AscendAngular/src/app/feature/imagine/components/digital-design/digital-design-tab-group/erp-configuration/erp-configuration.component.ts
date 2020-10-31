import { Component, OnInit } from "@angular/core";
import { DigitalDesignTabDataService } from 'src/app/feature/imagine/services/digital-design/digital-design-tab-data.service';
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
import { SharedService } from "src/app/shared/services/shared.service";
//Filter changes
import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: "app-erp-configuration",
  templateUrl: "./erp-configuration.component.html",
  styleUrls: ["./erp-configuration.component.scss"]
})
export class ErpConfigurationComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.DESIGN;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[6];
  toggledEvent: string;

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

    if (this.toggledEvent == "ERPCONFIG TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.digitalDesignTabDataService.configContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL3TabCount('', this.digitalDesignTabDataService.configContentsJson[0].tabContent, 'L3grp', 'workbookenabledflag','L2grp','workbookdoclink') });
  }

  setFilters() {
    //Filter changes
    this.digitalDesignTabDataService.configContentsJson = [];
    this.digitalDesignTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.digitalDesignTabDataService.configContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.emitTabCount();
    });
  }

  preview(doclink) {
    // window.location.href = doclink;
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, grandParent) {
    
    this.toggledEvent = "ERPCONFIG TOGGLED"

    // Flag toggle at L1 level
    if (j.L2grp != undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2grp) {
          if (i.L2enabledflag == "N") {
            i.L2enabledflag = "Y";
          }
          for (let k of i.L3grp) {
            if (k.workbookenabledflag == "N") {
              k.workbookenabledflag = "Y";
              this.sharedService.docAddEvent.emit(k.workbookenabledflag);
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
            if (k.workbookenabledflag == "Y") {
              k.workbookenabledflag = "N";
              this.sharedService.docAddEvent.emit(k.workbookenabledflag);
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
          if (i.workbookenabledflag == "N") {
            i.workbookenabledflag = "Y";
            this.sharedService.docAddEvent.emit(i.workbookenabledflag);
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
          if (i.workbookenabledflag == "Y") {
            i.workbookenabledflag = "N";
            this.sharedService.docAddEvent.emit(i.workbookenabledflag);
          }
        }
      }
    }
    //Flag toggle at L3 level
    else {
      if (j.workbookenabledflag == "N") {
        j.workbookenabledflag = "Y";
        this.sharedService.docAddEvent.emit(j.workbookenabledflag);
        if (parent.L3grp.find(t => t.workbookenabledflag == "Y") == undefined) {
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
        j.workbookenabledflag = "N";
        this.sharedService.docAddEvent.emit(j.workbookenabledflag);
        if (parent.L3grp.find(t => t.workbookenabledflag == "N") == undefined) {
          parent.L2enabledflag = "Y";
          if (
            grandParent.L2grp.find(t => t.L2enabledflag == "Y") == undefined
          ) {
            grandParent.L1enabledflag = "N";
          } else {
            grandParent.L1enabledflag = "Y";
          }
        } else if (
          parent.L3grp.find(t => t.workbookenabledflag == "Y") == undefined
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
