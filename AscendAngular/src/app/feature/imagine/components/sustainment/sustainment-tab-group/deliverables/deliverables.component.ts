import { Component, OnInit, Input } from '@angular/core';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { SustainmentTabDataService } from 'src/app/feature/imagine/services/sustainment/sustainment-tab-data.service';
//Filter changes
import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-deliverables',
  templateUrl: './deliverables.component.html',
  styleUrls: ['./deliverables.component.scss']
})
export class DeliverablesComponent implements OnInit {

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  filters: any;

  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.SUSTAINMENT;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[0];
  bgColor: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.colorScheme
  toggledEvent: string;

  constructor(
    private globalData: PassGlobalInfoService,
    private sharedService: SharedService,
    private sustainmentTabDataService: SustainmentTabDataService,
    private messagingService: MessagingService,//Filter changes
    private utilService: UtilService,//Filter changes
    public dialog: MatDialog,//Filter changes
  ) { }

  ngOnInit() {
    this.sustainmentTabDataService.clearData(); //Filter changes
    this.sustainmentTabDataService.clearFilters(); //Filter changes

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

    if (this.toggledEvent == "DELIVERABLES TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.sustainmentTabDataService.deliverablesContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL1TabCount('', this.sustainmentTabDataService.deliverablesContentsJson[0].tabContent, 'L1enabledflag','L1doclink') });
  }

  setFilters() {
    this.filters = [
      {
        filterValues: this.sustainmentTabDataService.agileManager,
        selectedFilters: {
          L0: this.sustainmentTabDataService.selectedAgileManager,
          L1: []
        },
        type: "A",
        title: { main: "Select delivery tools", levels: [] },
        filterButtonTitle: "Delivery tools"
      }
    ];

    //Filter changes
    this.sustainmentTabDataService.clearData();
    this.sustainmentTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.sustainmentTabDataService.deliverablesContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.sustainmentTabDataService.agileManager = this.utilService.formTechnologyFilter(this.sustainmentTabDataService.deliverablesContentsJson[0].tabContent, this.sustainmentTabDataService.agileManager, "L1value");
      this.sustainmentTabDataService.deliverablesContentsJson[0].tabContent = this.utilService.technologyFilters(this.sustainmentTabDataService.deliverablesContentsJson[0].tabContent, this.sustainmentTabDataService.selectedAgileManager, "L1value");
      this.emitTabCount()
    });

  }

  eventEmit(e) {
    this.sustainmentTabDataService.setSelectedFilter(e).subscribe(data => { this.setFilters()});
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, flag) {

    this.toggledEvent = "DELIVERABLES TOGGLED"
    if (j.L1enabledflag == 'N') {
      j.L1enabledflag = 'Y'
      this.sharedService.docAddEvent.emit(j.L1enabledflag);
    } else {
      j.L1enabledflag = 'N'
      this.sharedService.docAddEvent.emit(j.L1enabledflag);
    }
    this.emitTabCount()
    event.stopPropagation();
  }

}
