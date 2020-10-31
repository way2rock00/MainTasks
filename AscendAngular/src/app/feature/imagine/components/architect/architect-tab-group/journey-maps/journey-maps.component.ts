import { Component, OnInit, Input, OnDestroy } from '@angular/core';
import { SharedService } from 'src/app/shared/services/shared.service';
import { ArchitectTabDataService } from '../../../../services/architect/architect-tab-data.service';
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
  selector: 'app-journey-maps',
  templateUrl: './journey-maps.component.html',
  styleUrls: ['./journey-maps.component.scss']
})
export class JourneyMapsComponent implements OnInit, OnDestroy {

  filters: any;
  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.ARCHITECT;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[1];
  bgColor: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.colorScheme;
  toggledEvent: string;

  constructor(
    private globalData: PassGlobalInfoService,
    private sharedService: SharedService,
    private architectTabDataService: ArchitectTabDataService,
    private messagingService: MessagingService,//Filter changes
    private utilService: UtilService,//Filter changes
    public dialog: MatDialog,//Filter changes
  ) { }

  ngOnInit() {

    this.architectTabDataService.clearData(); //Filter changes
    this.architectTabDataService.clearFilters(); //Filter changes

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

    if (this.toggledEvent == "JOURNEYMAPS TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.architectTabDataService.journeyMapsContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL2TabCount('', this.architectTabDataService.journeyMapsContentsJson[0].tabContent, 'L2Grp', 'L2enabledflag','L2doclink') });
  }

  setFilters() {
    this.filters = [
      {
        filterValues: this.architectTabDataService.journeyMaps,
        selectedFilters: {
          L0: this.architectTabDataService.selectedjourneyMaps,
          L1: []
        },
        type: "J",
        title: { main: "Select Personas", levels: [] },
        filterButtonTitle: "personas"
      }
    ];

    //Filter changes move all logic to component level
    this.architectTabDataService.journeyMapsContentsJson = [];
    this.architectTabDataService.getTabDataURL(`${this.tab.serviceURL}`).subscribe(data => {
      this.architectTabDataService.journeyMapsContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.architectTabDataService.journeyMaps = this.utilService.formadvancedFilter(this.architectTabDataService.journeyMapsContentsJson[0].tabContent, this.architectTabDataService.journeyMaps, "L2Grp");
      this.architectTabDataService.journeyMapsContentsJson[0].tabContent = this.utilService.advancedFilters(this.architectTabDataService.journeyMapsContentsJson[0].tabContent, this.architectTabDataService.selectedjourneyMaps, "L2Grp");
      this.emitTabCount();
    });

  }

  eventEmit(e) {
    var self = this;
    this.architectTabDataService.setSelectedFilter(e).then(function (data) {
      self.setFilters();
    });
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, grandParent, flag) {
    this.toggledEvent = "JOURNEYMAPS TOGGLED";//Filter changes

    // Flag toggle at L1 level
    if (j.L2Grp != undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2Grp) {
          if (i.L2enabledflag == "N") {
            i.L2enabledflag = "Y";
            this.sharedService.docAddEvent.emit(i.L2enabledflag);
            
          }
          for (let k of i.L3Grp) {
            if (k.JouneyenabledFlag == "N") {
              k.JouneyenabledFlag = "Y";
              
            }
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.L2Grp) {
          if (i.L2enabledflag == "Y") {
            i.L2enabledflag = "N";
            this.sharedService.docAddEvent.emit(i.L2enabledflag);
          }
          for (let k of i.L3Grp) {
            if (k.JouneyenabledFlag == "Y") {
              k.JouneyenabledFlag = "N";
              
            }
          }
        }
      }
    }

    //Flag toggle at L2 level
    else if (grandParent == undefined) {
      if (j.L2enabledflag == "N") {
        j.L2enabledflag = "Y";
        this.sharedService.docAddEvent.emit(j.L2enabledflag);
        if (parent.L2Grp.find(t => t.L2enabledflag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
        }
        for (let i of j.L3Grp) {
          if (i.JouneyenabledFlag == "N") {
            i.JouneyenabledFlag = "Y";
          }
        }
      } else {
        j.L2enabledflag = "N";
        this.sharedService.docAddEvent.emit(j.L2enabledflag);
        if (parent.L2Grp.find(t => t.L2enabledflag == "N") == undefined) {
          parent.L1enabledflag = "Y";
        } else if (
          parent.L2Grp.find(t => t.L2enabledflag == "Y") == undefined
        ) {
          parent.L1enabledflag = "N";
        }
        for (let i of j.L3Grp) {
          if (i.JouneyenabledFlag == "Y") {
            i.JouneyenabledFlag = "N";
          }
        }
      }
    }

    this.emitTabCount();
    event.stopPropagation();
  }

}
