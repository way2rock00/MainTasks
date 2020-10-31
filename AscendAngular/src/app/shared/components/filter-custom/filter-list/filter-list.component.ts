import { Component, OnInit, EventEmitter, Output, Input } from '@angular/core';
import { FilterOverlay } from '../../filter-overlay/filter-overlay.service';
import { FilterContentComponent } from '../../filter-custom/filter-content/filter-content.component';
import { FilterData, FilterArray } from '../../../model/filter-content.model';
import { Router } from '@angular/router';

@Component({
  selector: 'app-filter-list',
  templateUrl: './filter-list.component.html',
  styleUrls: ['./filter-list.component.scss']
})
export class FilterListComponent implements OnInit {

  @Output() parentEmitter = new EventEmitter();
  @Input() filters: any[];
  @Input() filterBarHead: string;
  @Input() filterBarHeadColor: any;
  @Input() filterLoaded: boolean;
  @Input() tabName: string;
  @Input() readOnly: boolean;

  hoveredString: string;

  constructor(private filterOverlay: FilterOverlay,private router: Router) { }

  ngOnInit() { }

  filterClicked(event, filterObj: FilterData) {

    filterObj.readOnly = this.readOnly;
    const ref = this.filterOverlay.open<FilterData>({
      content: FilterContentComponent,
      origin: event.target.parentElement,
      data: filterObj,
      height: '0%'

    });

    ref.afterClosed$.subscribe(res => {
      if (res.data) {
        this.parentEmitter.emit(res.data);
      }
    });
  }

  onHover(filterObj: FilterData) {

    this.hoveredString = "";

    this.hoveredString += this.formHoverString(filterObj.l1Filter.filterValues);
    this.hoveredString += this.formHoverString(filterObj.l2Filter.filterValues);
    this.hoveredString += this.formHoverString(filterObj.l3Filter.filterValues);
    this.hoveredString += this.formHoverString(filterObj.l4Filter.filterValues);
  }

  formHoverString(filterArray: FilterArray[]) {

    let title = "";
    let hoveredArray = [];

    if (filterArray) {
      for (let obj of filterArray) {
        if (obj.childValues) {
          for (let childEle of obj.childValues) {
            if (hoveredArray.indexOf(childEle.entityName) == -1 && (childEle.selectedFlag == 'Y')) {
              if (!title)
                title = childEle.entityType + ": ";
              hoveredArray.push(childEle.entityName)
            }
          }
        }
      }

      if (hoveredArray) {
        title += hoveredArray.join(",") + "  "
      }
    }

    return title;
  }
}