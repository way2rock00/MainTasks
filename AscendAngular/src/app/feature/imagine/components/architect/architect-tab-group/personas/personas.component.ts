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
  selector: 'app-personas',
  templateUrl: './personas.component.html',
  styleUrls: ['./personas.component.scss']
})
export class PersonasComponent implements OnInit, OnDestroy {

  filters: any;
  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;

  //Filter changes
  filterSubscription: Subscription;
  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.ARCHITECT;
  tab: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV].right.tabs[0];
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

    if (this.toggledEvent == "PERSONAS TOGGLED") {

      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tab.tabName,
          sessionStorageLabel: this.tab.tabStorage,
          tabContents: this.architectTabDataService.personasContentsJson,
          URL: `${this.tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";
  }

  emitTabCount() {
    //Filter changes Emit updated count
    this.sharedService.tabCountEvent.emit({ "tabName": this.tab.tabName, "tabCount": this.utilService.getL2TabCount('', this.architectTabDataService.personasContentsJson[0].tabContent, 'L2Grp', 'L2enabledflag','L2doclink') });
  }

  setFilters() {
    this.filters = [
      {
        filterValues: this.architectTabDataService.personas,
        selectedFilters: {
          L0: this.architectTabDataService.selectedPersonas,
          L1: []
        },
        type: "P",
        title: { main: "Select personas", levels: [] },
        filterButtonTitle: "Personas"
      }
    ];

    //Filter changes
    this.architectTabDataService.personasContentsJson = [];
    this.architectTabDataService.getTabDataURL(this.tab.serviceURL).subscribe(data => {
      this.architectTabDataService.personasContentsJson = this.utilService.formTabContents(data, this.tab.tabName, this.tab.tabStorage);
      this.architectTabDataService.personas = this.utilService.formadvancedFilter(this.architectTabDataService.personasContentsJson[0].tabContent, this.architectTabDataService.personas, "L2Grp");
      this.architectTabDataService.personasContentsJson[0].tabContent = this.utilService.advancedFilters(this.architectTabDataService.personasContentsJson[0].tabContent, this.architectTabDataService.selectedPersonas, "L2Grp");
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
    // window.location.href = doclink;
    window.open(doclink);
    event.stopPropagation();
  }

  toggle(j, parent, flag) {
    this.toggledEvent = "PERSONAS TOGGLED";//Filter changes

    if (parent == undefined) {
      if (j.L1enabledflag == "N") {
        j.L1enabledflag = "Y";
        for (let i of j.L2Grp) {
          if (i.L2enabledflag == 'N') {
            i.L2enabledflag = "Y";
            this.sharedService.docAddEvent.emit(i.L2enabledflag);
          }
        }
      } else {
        j.L1enabledflag = "N";
        for (let i of j.L2Grp) {
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
        if (parent.L2Grp.find(t => t.L2enabledflag == "Y") == undefined) {
          parent.L1enabledflag = "N";
        } else {
          parent.L1enabledflag = "Y";
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
      }
    }
    this.emitTabCount();
    event.stopPropagation();
  }

}
