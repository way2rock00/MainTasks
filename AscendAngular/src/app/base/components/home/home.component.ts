import { CryptUtilService } from './../../../shared/services/crypt-util.service';
import { Component, OnInit, Output, EventEmitter } from "@angular/core";
import { OwlOptions } from "ngx-owl-carousel-o";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { Router } from "@angular/router";
import { UserInfo } from "src/app/shared/constants/ascend-user-project-info";

import { HOME_CONSTANT } from '../../constants/home-constant';
import { FilterCustomService } from 'src/app/shared/services/filter-custom.service';
import { FilterData } from 'src/app/shared/model/filter-content.model';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { FILTER_CUSTOM_CONSTANTS } from 'src/app/shared/model/filter-content.model';
import { Subscription } from 'rxjs';
import { SharedService } from 'src/app/shared/services/shared.service';


@Component({
  selector: "app-home",
  templateUrl: "./home.component.html",
  styleUrls: ["./home.component.scss"]
})
export class HomeComponent implements OnInit {
  //PageTitle:string="Ascend - Home Page";

  projectGlobalInfo: ProjectGlobalInfoModel;
  customOptions: OwlOptions = {
    loop: false,
    dots: false,
    // navSpeed: 700,
    //items:1,
    //margin:25,
    //center: false,
    nav: true,
    navText: ["<img src='../../../../assets/left@3x.png' width='30px' />",
      "<img src='../../../../assets/Right@3x.png' width='30px' />"],
    stagePadding: 45
    //autoWidth: true
  };
  router: any;
  view: String;
  constUserInfo: UserInfo;
  projectName: any;
  clientName: any;
  clientLogoURL: any;
  //cardData: any= LAYOUT_CONFIGURATION;
  cardData: any = HOME_CONSTANT;

  filterURL = "/iidrfilter/";
  filters: FilterData[] = [];
  storageConstant: string;
  filterLoaded = false;
  tabName = "IIDR";
  subscription: Subscription;
  urlTrailer: string = '';

  constructor(
    private globalData: PassGlobalInfoService,
    private filterCustomService: FilterCustomService,
    private messagingService: MessagingService,
    private cryptUtilService: CryptUtilService,
    private sharedService: SharedService
  ) { }

  ngOnInit() {
    this.subscription = this.globalData.share.subscribe(data => {
      this.view = data.viewMode;
      this.clientName = data.clientName;
      this.projectName = data.projectName;
      this.clientLogoURL = data.clientUrl;
      this.projectGlobalInfo = data;
      this.projectGlobalInfo.projectId = this.projectGlobalInfo.projectId ? this.projectGlobalInfo.projectId : '0';
      this.storageConstant = FILTER_CUSTOM_CONSTANTS.IIDR_FILTER + "_" + this.projectGlobalInfo.projectId;
      this.emitFilter(null);
    });
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  emitFilter(e) {
    if (e) {
      this.filterCustomService.updateFilters(this.filters, e, this.storageConstant);
      this.urlTrailer = this.filterCustomService.formURL(this.filters);
      this.cryptUtilService.setItem(BUS_MESSAGE_KEY.IIDR_FILTER + "_functionpackage_" + this.projectGlobalInfo.projectId, this.urlTrailer, 'SESSION');
      this.sharedService.filterSelected = this.filterCustomService.checkFilterSelected(this.filters);
      // this.messagingService.publish(BUS_MESSAGE_KEY.IIDR_FILTER, urlTrailer)
    }
    else {
      this.filterCustomService.getFilterData(this.filterURL + this.projectGlobalInfo.projectId, this.storageConstant).subscribe(data => {
        this.filters = data;

        //Baxter Project Package change
        if(this.filters && this.projectName.toLowerCase() == 'baxter jde next generation erp program'){
          this.filters[0].l1Filter.filterValues[0].childValues[0].entityName = 'JD Edwards'
        }
        console.log(this.filters);
        
        this.filterLoaded = true;
        this.urlTrailer = this.filterCustomService.formURL(this.filters);
        this.cryptUtilService.setItem(BUS_MESSAGE_KEY.IIDR_FILTER + "_functionpackage_" + this.projectGlobalInfo.projectId, this.urlTrailer, 'SESSION');
        this.sharedService.filterSelected = this.filterCustomService.checkFilterSelected(this.filters);
        // this.messagingService.publish(BUS_MESSAGE_KEY.IIDR_FILTER, urlTrailer);
      });
    }
  }
}
