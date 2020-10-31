import { Component, OnInit } from '@angular/core';
import { DigitalDesignTabDataService } from 'src/app/feature/imagine/services/digital-design/digital-design-tab-data.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { SharedService } from 'src/app/shared/services/shared.service';
//Filter changes
import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: "app-key-business-decisions",
  templateUrl: "./key-business-decisions.component.html",
  styleUrls: ["./key-business-decisions.component.scss"]
})
export class KeyBusinessDecisionsComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.DESIGN;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[3];
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

    if (this.toggledEvent == "KBD TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.digitalDesignTabDataService.leadingPracticesContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL2TabCount('', this.digitalDesignTabDataService.leadingPracticesContentsJson[0].tabContent, 'L2grp', 'L2enabledflag','L2doclink') });
  }

  setFilters() {
    //Filter changes
    this.digitalDesignTabDataService.leadingPracticesContentsJson = [];
    this.digitalDesignTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.digitalDesignTabDataService.leadingPracticesContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.emitTabCount();
    });
  }

  preview(doclink) {
    // window.location.href = doclink;
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent) {
    
    this.toggledEvent = "KBD TOGGLED";
    if (parent == undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2grp) {
          i.L2enabledflag = "Y";
          this.sharedService.docAddEvent.emit(i.L2enabledflag);
          for (let k of i.L3grp) {
            if (k.decisionenabledFlag == 'N') {
              k.decisionenabledFlag = 'Y';
            }
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.L2grp) {
          i.L2enabledflag = "N";
          this.sharedService.docAddEvent.emit(i.L2enabledflag);

          for (let k of i.L3grp) {
            if (k.decisionenabledFlag == 'Y') {
              k.decisionenabledFlag = 'N';    
            }
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
        for (let k of j.L3grp) {
          if (k.decisionenabledFlag == 'N') {
            k.decisionenabledFlag = 'Y';
            
          }
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
        for (let k of j.L3grp) {
          if (k.decisionenabledFlag == 'Y') {
            k.decisionenabledFlag = 'N';
          }
        }
      }
    }

    this.emitTabCount();
    event.stopPropagation();
  }
}
