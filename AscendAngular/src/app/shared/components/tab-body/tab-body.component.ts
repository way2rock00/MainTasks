import { Component, OnInit, Input, SimpleChange, OnDestroy } from '@angular/core';
import { FilterCustomService } from '../../services/filter-custom.service';
import { FilterData, FILTER_CUSTOM_CONSTANTS } from '../../model/filter-content.model';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { environment } from 'src/environments/environment';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-tab-body',
  templateUrl: './tab-body.component.html',
  styleUrls: ['./tab-body.component.scss']
})
export class TabBodyComponent implements OnInit, OnDestroy {

  @Input() tabObj: any;
  @Input() functionPackageURL: string;
  @Input() colorScheme: string;
  @Input() activityId: string;

  filters: FilterData[] = [];
  filterAPI: string;
  filterLoaded = false;
  storageConstant: string;
  projectSubscription: Subscription

  projectGlobalInfo: ProjectGlobalInfoModel;
  contentBaseURL: string;
  getContentAPI: string;
  postContentAPI: string;

  constructor(private filterCustomService: FilterCustomService, private globalData: PassGlobalInfoService) { }

  ngOnInit() {

    this.projectSubscription = this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.projectGlobalInfo.projectId = this.projectGlobalInfo.projectId ? this.projectGlobalInfo.projectId : '0';
      this.projectGlobalInfo.uniqueId = this.projectGlobalInfo.uniqueId ? this.projectGlobalInfo.uniqueId : '0';
      this.tabChanged();
    });
  }

  ngOnChanges(changes: { [propKey: string]: SimpleChange }) {
    if (!changes['tabObj'].isFirstChange) {
      if (this.tabObj)
        this.tabChanged();
    }
  }

  ngOnDestroy() {
    this.projectSubscription.unsubscribe();
  }

  tabChanged() {
    if (this.tabObj) {
      this.filters = [];
      this.filterAPI = `${this.tabObj.filterAPI}${this.projectGlobalInfo.projectId}/${this.tabObj.contentId}`;
      this.storageConstant = FILTER_CUSTOM_CONSTANTS.DELIVERABLES + "_" + this.activityId + "_" + this.tabObj.contentId + "_" + this.projectGlobalInfo.projectId;
      this.functionPackageURL = this.functionPackageURL.charAt(0) == '/' ? this.functionPackageURL.slice(1) : this.functionPackageURL;
      this.contentBaseURL = `${environment.BASE_URL}${this.tabObj.contentAPI}${this.functionPackageURL}/${this.activityId}/${this.projectGlobalInfo.uniqueId}`;
      this.getContentAPI = "";
      this.postContentAPI = `${this.tabObj.contentAPI}`;
      this.emitFilter(null);
    }
  }

  formContentAPI() {
    this.getContentAPI = `${this.contentBaseURL}${this.filterCustomService.formURL(this.filters)}`;
  }

  emitFilter(e) {

    if (e) {
      this.filterCustomService.updateFilters(this.filters, e, this.storageConstant);

      if (e.l1Filter.advFilterApplicable == 'N') {
        //Filter changed is not advanced filter
        if (this.filterCustomService.IsAdvancedFilterApplicable(this.filters)) {
          //Advanced filter is applicable for tab, refresh advanced filter
          let advancedFilterURL = this.filterCustomService.formURL(this.filters, FILTER_CUSTOM_CONSTANTS.ADVANCED_FILTER);
          let advancedAPI = `${this.tabObj.advanceFilterAPI}${this.projectGlobalInfo.projectId}/${this.tabObj.contentId}${advancedFilterURL}`;

          this.filterCustomService.getFilterData(advancedAPI, '').subscribe(data => {
            if (data)
              for (let element of data)
                this.filterCustomService.updateFilters(this.filters, element, this.storageConstant, FILTER_CUSTOM_CONSTANTS.ADVANCED_FILTER);
            this.formContentAPI();
          });
        }
        else {
          //Advanced filter is not applicable for tab
          this.formContentAPI();
        }
      }
      else {
        //Filter changed is advanced filter
        this.formContentAPI();
      }
    }

    else {
      this.filterCustomService.getFilterData(this.filterAPI, this.storageConstant).subscribe(data => {
        this.filters = data;
        this.filterLoaded = true;
        this.formContentAPI();
      });
    }
  }
}