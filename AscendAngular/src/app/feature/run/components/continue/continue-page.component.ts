import { Component, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
import { LAYOUT_RUN_SUB_NAV, LAYOUT_TYPE, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { FilterOverlayService } from 'src/app/shared/services/filter-overlay.service';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { ContinueTabDataService } from '../../services/continue/continue-tab-data.service';

@Component({
  selector: 'app-continue-page',
  templateUrl: './continue-page.component.html',
  styleUrls: ['./continue-page.component.scss']
})
export class ContinuePageComponent implements OnInit {

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.RUN;
  SUB_NAV: LAYOUT_RUN_SUB_NAV = LAYOUT_RUN_SUB_NAV.CONTINUE;
  layoutConfig: any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV];

  filters: any = [];
  filterSubscription: Subscription;
  currentTabCode: any; //Filter Changes

  constructor(
    private filterOverlayService: FilterOverlayService,
    private continueTabDataService: ContinueTabDataService,
    private messagingService: MessagingService,
    private sharedService: SharedService) { }

  ngOnInit() {

    //clear filters from previous if any
    this.messagingService.publish(BUS_MESSAGE_KEY.GLOBAL_FILTER, null);

    //subscribe to filter change
    this.filterSubscription = this.messagingService
      .subscribe(BUS_MESSAGE_KEY.GLOBAL_FILTER, data => this.onFilterChange(data))

    this.sharedService.selectedPageEvent.emit(this.LAYOUT);
  }

  setFilters() {

    this.filters = [

      {
        filterValues: this.filterOverlayService.industries,
        selectedFilters: { L0: this.filterOverlayService.selectedIndustry, L1: this.filterOverlayService.selectedSectors },
        type: "I",
        title: { main: "Select industries & sectors", levels: ["Industries", "Sectors"] },
        filterButtonTitle: "Industries & sectors"
      },
      {
        filterValues: this.filterOverlayService.businessProcess,
        selectedFilters: {
          L0: this.filterOverlayService.selectedL0,
          L1: this.filterOverlayService.selectedL1,
          L2: this.filterOverlayService.selectedL2
        },
        type: "B",
        title: { main: "Select processes", levels: ["L1 processes", "L2 processes", "L3 processes"] },
        filterButtonTitle: "Processes"
      },
      {
        filterValues: this.filterOverlayService.regions,
        selectedFilters: { L0: this.filterOverlayService.selectedRegions },
        type: "r",
        title: { main: "Select regions", levels: [] },
        filterButtonTitle: "Regions"
      }
    ]
    // console.log('digital', this.filters);
  }

  onFilterChange(e) {
    if (this.currentTabCode) {  //Filter Changes

      //Filter changes, Store tab code to ensure only latest value is received from component
      let tabCode = this.currentTabCode;
      if (e) {
        this.continueTabDataService
          .setSelectedFilter(e)
          .subscribe(() => {
            this.setFilters();
            this.messagingService.publish(BUS_MESSAGE_KEY.FILTER_CHANGED, tabCode)
          })//Filter changes

      } else {
        this.filterOverlayService
          .setFilterData(this.currentTabCode, this.SUB_NAV) //Filter Changes
          .then((data) => this.setFilters())
          .then(() => this.messagingService.publish(BUS_MESSAGE_KEY.FILTER_CHANGED, tabCode))
      }
    }
  }

  //Filter Changes
  tabChanged(e) {
    if (this.currentTabCode != e) {
      this.currentTabCode = e;
      this.filters = [];
      this.messagingService.publish(BUS_MESSAGE_KEY.GLOBAL_FILTER, null);
    }
  }

  ngOnDestroy() {
    this.filterSubscription.unsubscribe();
  }

}
