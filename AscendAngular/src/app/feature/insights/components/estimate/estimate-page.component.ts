import { EstimateTabDataService } from './../../services/estimate/estimate-tab-data.service';
import { Component, OnInit } from '@angular/core';
import { LAYOUT_TYPE, LAYOUT_INSIGHTS_SUB_NAV } from 'src/app/shared/constants/layout-constants';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { Subscription } from 'rxjs';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { FilterOverlayService } from 'src/app/shared/services/filter-overlay.service';
import { EstablishTabDataService } from '../../services/establish/establish-tab-data.service';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';

@Component({
  selector: 'app-estimate-page',
  templateUrl: './estimate-page.component.html',
  styleUrls: ['./estimate-page.component.scss']
})
export class EstimatePageComponent implements OnInit {

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.INSIGHTS;
  SUB_NAV: LAYOUT_INSIGHTS_SUB_NAV = LAYOUT_INSIGHTS_SUB_NAV.ESTIMATE;

  view: any;
  projectGlobalInfo: ProjectGlobalInfoModel;
  filters: any;
  filterSubscription: Subscription;

  constructor(
    private globalData: PassGlobalInfoService,
    private filterOverlayService: FilterOverlayService,
    private estimateTabDataService: EstimateTabDataService,
    private messagingService: MessagingService,
    private sharedService: SharedService) { }

  ngOnInit() {
    this.globalData.share.subscribe(x => (this.projectGlobalInfo = x));
    this.view = this.projectGlobalInfo.viewMode;

    this.filterOverlayService.setFilterData().then((data) => {
      this.setFilters();
    });

    //clear filters from previous if any
    this.messagingService.publish(BUS_MESSAGE_KEY.GLOBAL_FILTER, null);

    //subscribe to filter change
    // this.filterSubscription = this.messagingService
    // .subscribe(BUS_MESSAGE_KEY.GLOBAL_FILTER, data => this.onFilterChange(data))

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

  // onFilterChange(e){
  //   if (e) {
  //     this.estimateTabDataService
  //     .setSelectedFilter(e)
  //     .then((data) => this.setFilters());
  //   } else {
  //     this.filterOverlayService
  //     .setFilterData()
  //     .then(() => this.setFilters());
  //   }
  // }

  // ngOnDestroy() {
  //   this.filterSubscription.unsubscribe();
  // }

}
