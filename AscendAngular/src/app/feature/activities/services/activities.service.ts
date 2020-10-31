import { Injectable, EventEmitter } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ActivitiesService {

  constructor() { }

  public hierarchyDetEvent = new EventEmitter();

  constructFilter(constructedFilter, filterObj, assigned) {

    if (filterObj.parentHierarchyId == null) {
      filterObj.children = [];
      constructedFilter.push(filterObj);
      return true;
    } else if (filterObj.parentHierarchyId == constructedFilter.hierarchyId && filterObj.dataVisible == 'N') {
      filterObj.children = [];
      constructedFilter.children.push(filterObj);
      return true;
    }
    else {
      if (constructedFilter.children != undefined) {
        for (let index = 0; index < constructedFilter.children.length; index++) {
          let constructedObj = constructedFilter.children[index];
          if (assigned == false) {
            assigned = this.constructFilter(constructedObj, filterObj, assigned);
          }
        }
      } else {
        for (let index = 0; index < constructedFilter.length; index++) {
          let constructedObj = constructedFilter[index];
          if (assigned == false) {
            assigned = this.constructFilter(constructedObj, filterObj, assigned);
          }
        }
      }
      return false;
    }
  }

  filterConstruct(filterData) {
    let constructedFilter = [];
    for (let i of filterData) {
      let filterObj = i;
      let assigned = false;
      this.constructFilter(constructedFilter, filterObj, assigned)
    }
    return constructedFilter;
  }

}
