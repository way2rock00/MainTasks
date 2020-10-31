import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { of, Observable } from 'rxjs';
import { formFilterArray, FilterData, FILTER_CUSTOM_CONSTANTS, FilterArray } from '../model/filter-content.model';
import { environment } from 'src/environments/environment';
import { filter } from 'rxjs/operators';
import { CryptUtilService } from './crypt-util.service';
import { Session } from 'protractor';

@Injectable({
  providedIn: 'root'
})
export class FilterCustomService {

  constructor(private http: HttpClient, private cryptUtilService: CryptUtilService) { }

  getFilterData(filterURL, storageKey): Observable<any> {
    if (storageKey) {
      let data = this.cryptUtilService.getItem(storageKey, 'SESSION');
      if (data) return of(data);
    }
    return this.http.get<any>(`${environment.BASE_URL}${filterURL}`).map(data => {
      let customComponentArray = formFilterArray(data);
      if (storageKey) this.updateSessionStorage(customComponentArray, storageKey);
      return customComponentArray
    });
  }

  IsAdvancedFilterApplicable(filters: FilterData[]): boolean {
    return filters.findIndex(t => t.l1Filter.advFilterApplicable == 'Y') >= 0
  }

  updateSessionStorage(data, storageKey) {
    this.cryptUtilService.setItem(storageKey, data, 'SESSION');
  }

  formURL(filters: FilterData[], filterType?: string): string {
    let filterURL = "";

    for (let customComponent of filters) {
      filterURL += this.formCommaSeparatedString(customComponent, 1, filterType)
    }

    return filterURL;

  }

  appendToURL(filterType, filterObj) {
    return (
      filterType != FILTER_CUSTOM_CONSTANTS.ADVANCED_FILTER
      || (filterType == FILTER_CUSTOM_CONSTANTS.ADVANCED_FILTER && filterObj.l1Filter.advFilterApplicable == 'N')
    )
  }

  formCommaSeparatedString(filterObj: FilterData, level, filterType: string): string {
    let filterArray;
    let URL = "";

    switch (level) {
      case 1: filterArray = filterObj.l1Filter.filterValues; break;
      case 2: filterArray = filterObj.l2Filter.filterValues; break;
      case 3: filterArray = filterObj.l3Filter.filterValues; break;
      case 4: filterArray = filterObj.l4Filter.filterValues; break;
    }
    if (filterArray && this.appendToURL(filterType, filterObj)) {
      //check if filters were updated
      let changedFlag = (filterArray.findIndex(t => t.changed == 'Y') >= 0);
      if (changedFlag)
        for (let filterEle of filterArray) {
          if (filterEle.childValues)
            URL += Array.prototype.map.call(
              filterEle.childValues.filter(t => t.selectedFlag == 'Y'),
              s => s.entityId
            ).toString();
        }

      URL = (URL ? "/" + URL : "/0") + this.formCommaSeparatedString(filterObj, level + 1, filterType);
    }
    return URL
  }

  updateFilters(originalData: FilterData[], filterComponent: FilterData, storageKey: string, filterType?: string) {

    let index = originalData.findIndex(t => t.l1Filter.filterId === filterComponent.l1Filter.filterId);
    if (index != -1) {
      // if (filterComponent.l1Filter.advFilterApplicable == 'Y' && filterType == FILTER_CUSTOM_CONSTANTS.ADVANCED_FILTER) { //Check if custom component is for advanced filter
      //   this.updateSelectedFlag(filterComponent.l1Filter.filterValues, originalData[index].l1Filter.filterValues);
      //   this.updateSelectedFlag(filterComponent.l2Filter.filterValues, originalData[index].l2Filter.filterValues);
      //   this.updateSelectedFlag(filterComponent.l3Filter.filterValues, originalData[index].l3Filter.filterValues);
      //   this.updateSelectedFlag(filterComponent.l4Filter.filterValues, originalData[index].l4Filter.filterValues);
      // }
      originalData[index] = filterComponent;
    }

    this.updateSessionStorage(originalData, storageKey);

    return originalData;
  }

  getSelectedFlag(filterValues: FilterArray[], childObj) {
    let selectedFlag = childObj ? childObj.selectedFlag : '';
    if (filterValues) {
      for (let ele of filterValues) {
        let index = ele.childValues.findIndex(t =>
          t.entityId == childObj.entityId &&
          t.entityType.toLowerCase() == childObj.entityType.toLowerCase());
        selectedFlag = index == -1 ? selectedFlag : ele.childValues[index].selectedFlag;
      }
    }

    return selectedFlag;
  }

  updateSelectedFlag(newFilterArray: FilterArray[], oldFilterArray: FilterArray[]) {
    if (newFilterArray && oldFilterArray) {
      for (let element of newFilterArray) {
        for (let l1Child of element.childValues) {
          l1Child.selectedFlag = this.getSelectedFlag(oldFilterArray, l1Child);
        }
      }
    }
    return newFilterArray;
  }

  checkFilterSelected(filters: FilterData[]): boolean {
    let checked = false;
    if (filters)
      for (let filterComponent of filters) {
        checked = false;
        if (filterComponent.l1Filter.filterValues)
          // check if atleast one filter value is selected for current filter object
          for (let obj of filterComponent.l1Filter.filterValues) {
            if(obj.childValues){
              let index = obj.childValues.findIndex(t => t.selectedFlag == 'Y');
              if (index != -1) {
                checked = true;
                break
              }
            }            
          }

        if ((!checked) && (filterComponent.l1Filter.filterValues))
          break;// filter found with no selected values return false
      }
    return checked
  }
}
