import { Component, OnInit } from '@angular/core';
import { SharedService } from 'src/app/shared/services/shared.service';
import { DigitalDesignTabDataService } from 'src/app/feature/imagine/services/digital-design/digital-design-tab-data.service';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
//Filter changes
import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-business-solution',
  templateUrl: './business-solution.component.html',
  styleUrls: ['./business-solution.component.scss']
})
export class BusinessSolutionComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.DESIGN;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[0];
  toggledEvent: string;

  constructor(private globalData: PassGlobalInfoService, private sharedService: SharedService,
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

    if (this.toggledEvent == "BUSINESSSOLUTIONS TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.digitalDesignTabDataService.businessSolutionContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL2TabCount('', this.digitalDesignTabDataService.businessSolutionContentsJson[0].tabContent, 'solutiongrp', 'solutionenabledFlag','solutiondoclink') });
  }

  setFilters() {
    //Filter changes
    this.digitalDesignTabDataService.businessSolutionContentsJson = [];
    this.digitalDesignTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.digitalDesignTabDataService.businessSolutionContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.emitTabCount();
    });
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent) {
    
    this.toggledEvent = "BUSINESSSOLUTIONS TOGGLED"
    if (parent == undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.solutiongrp) {
          if (i.solutionenabledFlag == 'N') {
            i.solutionenabledFlag = "Y";
            this.sharedService.docAddEvent.emit(i.solutionenabledFlag);
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.solutiongrp) {
          if (i.solutionenabledFlag == 'Y') {
            i.solutionenabledFlag = "N";
            this.sharedService.docAddEvent.emit(i.solutionenabledFlag);
          }
        }
      }
    } else {
      if (j.solutionenabledFlag == "N") {
        j.solutionenabledFlag = "Y";
        this.sharedService.docAddEvent.emit(j.solutionenabledFlag);
        if (parent.solutiongrp.find(t => t.solutionenabledFlag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
        }
      } else {
        j.solutionenabledFlag = "N";
        this.sharedService.docAddEvent.emit(j.solutionenabledFlag);
        if (parent.solutiongrp.find(t => t.solutionenabledFlag == "N") == undefined) {
          parent.L1enabledflag = "Y";
        } else if (
          parent.solutiongrp.find(t => t.solutionenabledFlag == "Y") == undefined
        ) {
          parent.L1enabledflag = "N";
        }
      }
    }
    this.emitTabCount()
    event.stopPropagation();
  }

}
