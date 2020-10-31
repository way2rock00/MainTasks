import { Component, OnInit } from '@angular/core';
import { MarketplaceService } from '../../services/marketplace.service';
import { filterConstruct } from '../../models/marketplace-filter-helper';
import { MarketplaceTools } from '../../models/marketplace-tools.model';
import { Subscription } from 'rxjs';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-marketplace',
  templateUrl: './marketplace.component.html',
  styleUrls: ['./marketplace.component.scss']
})
export class MarketplaceComponent implements OnInit {

  checkedId: any[] = [];
  //Changes for filter from new screen.
  disabledId: any[] = [];
  mode: string;
  constructedFilter = { "childs": [] };
  originalTools: MarketplaceTools[] = [];
  filteredTools: MarketplaceTools[] = [];
  toolsList: any[] = [];

  subscription: Subscription;

  constructor(private marketplaceService: MarketplaceService, private route: ActivatedRoute) { }

  ngOnInit() {

    //Changes for filter from new screen. Begin
    this.subscription = this.route.paramMap.subscribe(params => {
      let toolsUrl = params.get("tools");
      let fiterFromUrl = params.get("filters");
      this.mode = fiterFromUrl ? "deliverables" : '';

      if (this.mode.toLowerCase() == "deliverables") {
        let splitFilterId = fiterFromUrl.split(',');
        let splitToolsId = toolsUrl.split(',');
        for (let i = 0; i < splitFilterId.length; i++) {
          this.checkedId.push(parseInt(splitFilterId[i]));
          this.disabledId.push(splitFilterId[i]);
        }

        for (let i = 0; i < splitToolsId.length; i++) {
          this.toolsList.push(splitToolsId[i]);
        }
      }
    });

    this.marketplaceService.getFilters().subscribe(data => {
      this.constructedFilter = filterConstruct(data);
    });

    this.marketplaceService.getTools().subscribe(data => {
      this.originalTools = data;
      this.filterTools();
    });
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  //Changes for filter from new screen. Begin
  getCheckedState(id) {
    if (this.mode != 'deliverables')
      return false
    else {
      for (var element of this.checkedId) {
        if (element == id)
          return true;
      }
      return false;
    }
  }

  getDisabledState(id) {
    if (this.mode != 'deliverables')
      return false
    else {
      for (let element of this.disabledId) {
        if (element == id)
          return false;
      }
      return true;
    }
  }

  //Changes for filter from new screen. End

  filterTools() {
    this.filteredTools = [];
    for (let i of this.originalTools) {
      if(!i.filtersApplicable) i.filtersApplicable = [];
      if (this.checkedId.length == 0 || i.filtersApplicable.find(value => this.checkedId.includes(value))) {
        if (this.mode == "")
          this.filteredTools.push(i);
        else if (this.mode == 'deliverables' && this.toolsList.includes(i.toolId.toString()))
          this.filteredTools.push(i);
      }
    }
  }

  filterChangedEvent(event, id) {

    if (event.checked) {
      this.checkedId.push(id)
    }
    else {
      this.checkedId.splice(this.checkedId.indexOf(id), 1)
    }
    this.filterTools();
  }
}
