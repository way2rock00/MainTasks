import { Component, OnInit, Inject } from '@angular/core';
import { FilterOverlayRef } from '../../filter-overlay/filter-overlay-ref';
import { FilterData, FilterArray, FILTER_CUSTOM_CONSTANTS } from '../../../model/filter-content.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { AuthenticationService } from 'src/app/shared/services/authentication.service';

@Component({
  selector: 'app-filter-content',
  templateUrl: './filter-content.component.html',
  styleUrls: ['./filter-content.component.scss']
})
export class FilterContentComponent implements OnInit {

  filterComponent: FilterData;
  selectAll: string = "Select all";
  FILTER_CONSTANT = FILTER_CUSTOM_CONSTANTS;
  projectGlobalInfo: ProjectGlobalInfoModel;
  disabled: boolean;
  isAscendAdmin: boolean;

  constructor(private filterOverlayRef: FilterOverlayRef,
    private globalData: PassGlobalInfoService,
    private userService: AuthenticationService,
  ) {
    this.filterComponent = JSON.parse(JSON.stringify(filterOverlayRef.data));
  }

  ngOnInit() {
    this.isAscendAdmin = JSON.parse(this.userService.getUser().projectDetails.isAscendAdmin);
    this.globalData.share.subscribe(x => (this.projectGlobalInfo = x));
    this.disabled = (this.projectGlobalInfo.viewMode != 'EXPLORE' && this.filterComponent.readOnly) || (this.projectGlobalInfo.role == "PROJECT_MEMBER" && (!this.isAscendAdmin));
  }

  resetFilter() {
    for (let obj of this.filterComponent.l1Filter.filterValues)
      for (let childEle of obj.childValues) {
        childEle.selectedFlag = 'N';
        this.unCheckChildren(childEle, 2, 'N');
      }
  }

  selectAllFilters(event, childArray, parentArray: any[], level: number) {
    let selected = event.source.checked ? 'Y' : 'N';
    if (childArray) {
      for (let obj of childArray) {
        if ((!parentArray) || this.filterAdded(obj, parentArray)) //Check if filter is at first level or parent is selected
          obj.childValues = obj.childValues.map(t => {
            t.selectedFlag = selected;
            this.unCheckChildren(t, level + 1, selected)
            return t;
          });
        obj.changed = 'Y';
      }
    }
  }

  allSelected(childArray, parentArray) {
    let isUnChecked = false;

    if (childArray) {
      for (let obj of childArray) {
        if (((!parentArray) || this.filterAdded(obj, parentArray)) && obj.childValues) {
          isUnChecked = obj.childValues.findIndex(t => ((!t.selectedFlag) || t.selectedFlag == 'N')) >= 0;
          if (isUnChecked) break;
        }
      }
    }

    return (!isUnChecked)
  }

  filterChanged(event, element, level, selectType) {

    let filterArray = this.getFilterValues(level);
    let selectedFlag;
    if (event.source.checked) {
      selectedFlag = 'Y';
      //In case of single select deselect existing selected value      
      if (selectType == this.FILTER_CONSTANT.SINGLE) {
        for (let obj of filterArray) {
          if (obj.childValues) {
            let index = obj.childValues.findIndex(t => t.selectedFlag == 'Y');
            if (index >= 0) {
              obj.childValues[index].selectedFlag = 'N';
              this.unCheckChildren(obj.childValues[index], level + 1, 'N')
            }
          }
        }
      }
    }
    else {
      selectedFlag = 'N'
    }
    //Update current element and its children 
    element.selectedFlag = selectedFlag;
    if (selectedFlag == 'N' || selectType != this.FILTER_CONSTANT.SINGLE)
      this.unCheckChildren(element, level + 1, selectedFlag);

    filterArray = filterArray.map(t => {
      t.changed = 'Y';
      return t
    });
  }

  getFilterValues(level): FilterArray[] {
    switch (level) {
      case 1: return this.filterComponent.l1Filter.filterValues;
      case 2: return this.filterComponent.l2Filter.filterValues;
      case 3: return this.filterComponent.l3Filter.filterValues;
      case 4: return this.filterComponent.l4Filter.filterValues;
    }
  }

  unCheckChildren(element, level, flag) {
    let childObj;

    childObj = this.getFilterValues(level);

    if (childObj)
      for (let node of childObj) {
        if (element.entityId === node.entityId && element.entityType.toLowerCase() === node.entityType.toLowerCase()) {
          for (let childEle of node.childValues) {
            childEle.selectedFlag = flag;
            this.unCheckChildren(childEle, level + 1, flag);
          }
        }
      }
  }

  filterAdded(childElement, parentFilterArray) {
    //Find if parentId of child element is selected in parent element
    let isVisible = false;

    if (parentFilterArray)
      for (let x of parentFilterArray) {
        isVisible = x.childValues.findIndex(t => (t.entityId == childElement.entityId) &&
          (t.entityType.toLowerCase() == childElement.entityType.toLowerCase()) &&
          (t.selectedFlag == 'Y')) != -1
        if (isVisible)
          break;
      }
    return isVisible;
  }

  childElementPresent(childArray, parentFilterArray) {
    //Check if next level elements exist for selected parent
    return !!(
      (!!parentFilterArray) &&
      (!!childArray) &&
      childArray.filter(t => this.filterAdded(t, parentFilterArray)).length > 0
    );
  }

  apply(e) {
    this.filterOverlayRef.close(this.filterComponent);
  }

  cancel(e) {
    this.filterOverlayRef.close();
  }
}