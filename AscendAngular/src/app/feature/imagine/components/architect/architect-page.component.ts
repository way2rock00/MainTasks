import { SharedService } from 'src/app/shared/services/shared.service';
import { Component, OnInit, Input, OnDestroy } from '@angular/core';

import { LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV, LAYOUT_CONFIGURATION } from 'src/app/shared/constants/layout-constants';
import { FilterOverlayService } from 'src/app/shared/services/filter-overlay.service';
import { ArchitectTabDataService } from '../../services/architect/architect-tab-data.service';
import { Subscription } from 'rxjs';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';

@Component({
  selector: 'app-architect-page',
  templateUrl: './architect-page.component.html',
  styleUrls: ['./architect-page.component.scss']
})
export class ArchitectPageComponent implements OnInit, OnDestroy {

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.ARCHITECT;
  layoutConfig : any = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV];
  
  filters: any = [];
  filterSubscription: Subscription;
  currentTabCode: any; //Filter Changes

  constructor(
    private filterOverlayService: FilterOverlayService,
    private architectTabDataService: ArchitectTabDataService,
    private messagingService: MessagingService,
    private sharedService: SharedService) { }

  ngOnInit() {

    //clear tab data if any
    this.architectTabDataService.clearData();
    
    //clear filters from previous if any
    this.messagingService.publish(BUS_MESSAGE_KEY.GLOBAL_FILTER, null);

    //subscribe to filter change
    this.filterSubscription = this.messagingService
    .subscribe(BUS_MESSAGE_KEY.GLOBAL_FILTER, data => this.onFilterChange(data))

    this.sharedService.selectedPageEvent.emit(this.LAYOUT);
  }

  onFilterChange(e){
    if(this.currentTabCode){  //Filter Changes

      //Filter changes, Store tab code to ensure only latest value is received from component
      let tabCode = this.currentTabCode;
      if (e) {
        this.architectTabDataService
        .setSelectedFilter(e)
        .then(() => this.setFilters())
        //Filter changes
        .then(() => this.messagingService.publish(BUS_MESSAGE_KEY.FILTER_CHANGED, tabCode))
        
      } else {
          this.filterOverlayService
          .setFilterData( this.currentTabCode, this.SUB_NAV) //Filter Changes
          .then((data) => this.setFilters())
          .then(() => this.messagingService.publish(BUS_MESSAGE_KEY.FILTER_CHANGED, tabCode))
      }
    }
  }

  //Filter Changes
  tabChanged(e){
    if(this.currentTabCode != e){
      this.currentTabCode = e;
      this.filters = [];
      this.messagingService.publish(BUS_MESSAGE_KEY.GLOBAL_FILTER, null);
    }
  }

  setFilters(){

    this.filters = [

      {
        filterValues:this.filterOverlayService.industries,
        selectedFilters : {L0:this.filterOverlayService.selectedIndustry , L1: this.filterOverlayService.selectedSectors},
        type:"I",
        title:{ main: "Select industries & sectors", levels: ["Industries","Sectors"] },
        filterButtonTitle : "Industries & sectors"
      },
      {
        filterValues:this.filterOverlayService.businessProcess,
        selectedFilters : {
          L0:this.filterOverlayService.selectedL0 ,
          L1: this.filterOverlayService.selectedL1,
          L2:this.filterOverlayService.selectedL2
        },
        type:"B",
        title:{ main: "Select processes", levels: ["L1 processes", "L2 processes", "L3 processes"] },
        filterButtonTitle : "Processes"
      },
      {
        filterValues:this.filterOverlayService.regions,
        selectedFilters : {L0:this.filterOverlayService.selectedRegions},
        type:"r",
        title:{ main: "Select regions", levels: [] },
        filterButtonTitle : "Regions"
      }
    ];
  }

  ngOnDestroy() {
    this.filterSubscription.unsubscribe();
    this.architectTabDataService.clearData();
    this.architectTabDataService.clearFilters();
  }

}
