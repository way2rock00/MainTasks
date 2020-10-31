import { CryptUtilService } from './crypt-util.service';
import { Injectable } from '@angular/core';
import { FilterOverlayService } from "src/app/shared/services/filter-overlay.service";
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class UtilService {

  projectGlobalInfo: ProjectGlobalInfoModel;

  constructor(
    private cryptUtilService: CryptUtilService,
    private filterOverlayService: FilterOverlayService,
    private globalData: PassGlobalInfoService,
    private http: HttpClient) {
    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.projectGlobalInfo.uniqueId = x.uniqueId ? x.uniqueId : "0";
    });
  }

  formTabContents(tabData, tabName, tabStorageName) {
    let array = [];

    if (tabData == null) {
      tabData = [];
    }

    array[0] = {
      tabName: tabName,
      tabContent: tabData
    };

    this.cryptUtilService.setItem(tabStorageName, array, 'SESSION');

    return array;
  }

  joinFiltersURL(selectedFilters) {
    let urlString = "/"

    if (selectedFilters.length == 0)
      urlString = urlString + "0";
    else
      urlString = urlString + selectedFilters.map(function (e) { return e.Id }).join(",");

    return urlString;
  }

  setfilterParamsURL() {
    let url = this.projectGlobalInfo.uniqueId;
    url += this.joinFiltersURL(this.filterOverlayService.selectedIndustry);
    url += this.joinFiltersURL(this.filterOverlayService.selectedSectors);
    url += this.joinFiltersURL(this.filterOverlayService.selectedRegions);
    url += this.joinFiltersURL(this.filterOverlayService.selectedL0);
    url += this.joinFiltersURL(this.filterOverlayService.selectedL1);
    url += this.joinFiltersURL(this.filterOverlayService.selectedL2);

    return url;
  }

  formTechnologyFilter(tabData, filterData, eleName) {

    for (let j of tabData) {
      if (filterData.find((t) => t.L0 == j[eleName]) == undefined) {
        filterData.push({ L0: j[eleName], L1Map: [] });
      }
    }
    return filterData
  }

  technologyFilters(tabData, selectAdvancedFilter, eleName) {
    tabData = tabData.filter(e => {
      return (
        (selectAdvancedFilter.find(t => t.L0 == e[eleName])
          || e[eleName] == "All" || e[eleName] == "COMMON"
          || (selectAdvancedFilter.length == 0))
      )
    });

    return tabData
  }

  formadvancedFilter(tabData, filterData, eleName) {

    for (let j of tabData) {
      for (let k of j[eleName]) {
        if (filterData.find(t => t.L0 == k.L2value) == undefined) {
          filterData.push({ L0: k.L2value, L1Map: [] });
        }
      }
    }

    return filterData
  }

  advancedFilters(tabData, selectAdvancedFilter, eleName) {

    for (let j of tabData) {
      j[eleName] = j[eleName].filter(
        function (e) {
          return (
            selectAdvancedFilter.find(t => t.L0 == e.L2value) ||
            e.L2value == "All" ||
            e.L2value == "COMMON" ||
            selectAdvancedFilter.length == 0
          );
        });
    }
    tabData = tabData.filter(p => {
      return (p[eleName].length > 0)
    });

    return tabData
  }

  formL0Selection(filterArry) {
    let resultArray: any[] = [];
    filterArry.map(function (p) {
      if (p.checked) resultArray.push({ L0: p.L0, Id: p.L0Id });
    });

    return resultArray;
  }

  formL1Selection(filterArry) {
    let resultArray: any[] = [];
    filterArry.map(function (p) {
      resultArray.push({ L1: p.L1, Id: p.L1Id });
    });

    return resultArray;
  }

  formL2Selection(filterArry) {
    let resultArray: any[] = [];
    filterArry.map(function (p) {
      resultArray.push({ L2: p.L2, Id: p.L2Id });
    });

    return resultArray;
  }

  setSelectedFilter(e) {

    var self = this;

    switch (e.data.type) {

      case "I":
        self.filterOverlayService.selectedIndustry = [];
        self.filterOverlayService.selectedSectors = []

        self.filterOverlayService.selectedIndustry = this.formL0Selection(e.data.selectedfilterData.l0)
        self.filterOverlayService.selectedSectors = this.formL1Selection(e.data.selectedfilterData.l1);
        break;
      case "r":
        self.filterOverlayService.selectedRegions = [];
        self.filterOverlayService.selectedRegions = this.formL0Selection(e.data.selectedfilterData.l0)
        break;
      case "B":
        self.filterOverlayService.selectedL0 = [];
        self.filterOverlayService.selectedL1 = [];
        self.filterOverlayService.selectedL2 = [];

        self.filterOverlayService.selectedL0 = this.formL0Selection(e.data.selectedfilterData.l0)
        self.filterOverlayService.selectedL1 = this.formL1Selection(e.data.selectedfilterData.l1)
        self.filterOverlayService.selectedL2 = this.formL2Selection(e.data.selectedfilterData.l2)
        break;
    }

    return;
  }

  isGlobalFilter(val): boolean {
    return (val == "I" || val == "r" || val == "B")
  }

  getTabLabel(tabName, selCount, totCount) {
    return tabName + " (" + (this.projectGlobalInfo.viewMode == "PROJECT" ? selCount + "/" + totCount : totCount) + ")"
  }

  getL1TabCount(tabName, tabcontents, l1FlagName, doclink) {
    let totCount = 0;
    let selCount = 0;
    let tabLabel;

    if (tabcontents) {

      tabcontents.map(L1 => {
        if (L1[doclink]) {
          totCount += 1;
          if (L1[l1FlagName] == "Y")
            selCount += 1;
        }

      });
    }

    tabLabel = this.getTabLabel(tabName, selCount, totCount);

    return tabLabel;
  }

  getL2TabCount(tabName, tabcontents, l2EleName, l2FlagName, doclink) {
    let totCount = 0;
    let selCount = 0;
    let tabLabel;

    if (tabcontents) {
      tabcontents.map(L1 => {
        if (L1[l2EleName]) {

          L1[l2EleName].map(L2 => {
            if (L2[doclink]) {
              totCount += 1;
              if (L2[l2FlagName] == "Y")
                selCount += 1;
            }

          })
        }
      });
    }

    tabLabel = this.getTabLabel(tabName, selCount, totCount);

    return tabLabel;
  }

  getL3TabCount(tabName, tabcontents, l3EleName, l3FlagName, l2EleName, doclink) {
    let totCount = 0;
    let selCount = 0;
    let tabLabel;

    if (tabcontents) {
      tabcontents.map(L1 => {
        if (L1[l2EleName]) {
          L1[l2EleName].map(L2 => {
            if (L2[l3EleName]) {

              L2[l3EleName].map(L3 => {
                if (L3[doclink]) { //Added condition for doclink
                  totCount += 1;
                  if (L3[l3FlagName] == "Y")
                    selCount += 1;
                }
              });
            }
          });
        }
      });
    }

    tabLabel = this.getTabLabel(tabName, selCount, totCount);

    console.log(tabLabel);
    

    return tabLabel;
  }

  getTabInfo(URL): Observable<any[]> {
    return this.http.get<any>(`${environment.BASE_URL}${URL}`);
  }

  getTabDataURL(URL): Observable<any> {
    return this.http.get<any>(`${environment.BASE_URL}${URL}${this.setfilterParamsURL()}`);
  }
}
