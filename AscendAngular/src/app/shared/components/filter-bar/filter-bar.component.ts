import { SharedService } from './../../services/shared.service';
import { Component, OnInit, EventEmitter, Output, Input, ViewEncapsulation } from '@angular/core';
import { FilterOverlay } from '../filter-overlay/filter-overlay.service';
import { FilterSearchComponent } from '../filter-search/filter-search.component';

@Component({
  selector: 'app-filter-bar',
  templateUrl: './filter-bar.component.html',
  styleUrls: ['./filter-bar.component.scss'],
  encapsulation: ViewEncapsulation.None,
})
export class FilterBarComponent implements OnInit {

  constructor(private filterOverlay: FilterOverlay, private sharedService: SharedService) {
    this.sharedService.selectedTabEvent.subscribe(data => {
      this.tabName = data.tabName;
    })
  }

  @Output() parentEmitter = new EventEmitter();
  @Input() filters: any[];
  @Input() filterBarHead: string;
  @Input() filterBarHeadColor: any;
  @Input() isGlobal: any;
  hoveredContentL0: any[] = [];
  hoveredContentL1: any[] = [];
  hoveredContentL2: any[] = [];
  titleL0: any;
  titleL1: any;
  titleL2: any;
  tabName: string;

  filterButtonClicked(e, filterValues, selectedFilters, type, title) {

    // let levels = title.levels.length == 0 ? 1 : title.levels.length;

    const ref = this.filterOverlay.open<{
      initSelectedItems: any[], selectedfilterData: any, type: any,
      title: string, closeType: string, levels: number, isGlobal: boolean
    }>({
      content: FilterSearchComponent,
      origin: e.target.parentElement,
      data: {
        initSelectedItems: filterValues,
        selectedfilterData: selectedFilters,
        type: type,
        title: title,
        closeType: "Apply",
        levels: title.levels.length == 0 ? 1 : title.levels.length,
        isGlobal: this.isGlobal
      },
      // width: 'auto',
      height: '0%'

    });

    ref.afterClosed$.subscribe(res => {

      if (res.data && res.data.closeType != "Cancel") {
        this.parentEmitter.emit(res);
      }
    });
  }

  getSelectedCount() {
    let counter = 0;
    // let selectedCount = this.filterData.reduce((counter, {checked}) => checked ? counter + 1 : counter, 0)
  }

  onHover(selectedVals, title) {
    // console.log(selectedVals);
    this.hoveredContentL0 = [];
    this.hoveredContentL1 = [];
    this.hoveredContentL2 = [];
    this.titleL0 = "";
    this.titleL1 = "";
    this.titleL2 = "";
    if (selectedVals.L0.length > 0) {
      for (let i of selectedVals.L0) {
        if (i.L0 != undefined && this.hoveredContentL0.indexOf(i.L0) == -1) {
          if (title.levels[0] != undefined) {
            this.titleL0 = title.levels[0] + ": ";
          }
          this.hoveredContentL0.push(i.L0);
        }
      }
    } else {
      this.hoveredContentL0 = [];
      this.titleL0 = "";
    }
    if (selectedVals.L1 != undefined && selectedVals.L1.length > 0) {
      for (let j of selectedVals.L1) {
        if (j.L1 != undefined && this.hoveredContentL1.indexOf(j.L1) == -1) {
          this.titleL1 = title.levels[1] + ": ";
          this.hoveredContentL1.push(j.L1);
        }
      }
    } else {
      this.hoveredContentL1 = [];
      this.titleL1 = "";
    }
    if (selectedVals.L2 != undefined && selectedVals.L2.length > 0) {
      for (let k of selectedVals.L2) {
        if (this.hoveredContentL2.indexOf(k) == -1) {
          this.titleL2 = title.levels[2] + ": ";
          this.hoveredContentL2.push(k.L2);
        }
      }
    } else {
      this.hoveredContentL2 = [];
      this.titleL2 = "";
    }
  }

  ngOnInit() {
  }

  filterApplicable() {
    return !!(this.filters.length > 0 && (!this.filters.find(t => (t.filterValues && t.filterValues.length > 0))));
  }

}
