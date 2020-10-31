import { Component, OnInit } from '@angular/core';
import { MarketplaceSolutionsService } from '../../services/marketplacesolutions.service';
import { filterConstruct } from '../../models/marketplacesolutions-filter-helper';
import { MarketplaceSolutionTools } from '../../models/marketplacesolutions-tools.model';

@Component({
  selector: 'app-marketplacesolutions',
  templateUrl: './marketplacesolutions.component.html',
  styleUrls: ['./marketplacesolutions.component.scss']
})
export class MarketplacesolutionsComponent implements OnInit {
  checkedId: any[] = [];
  constructedFilter = { "childs": [] };
  originalTools: MarketplaceSolutionTools[] = [];
  filteredTools: MarketplaceSolutionTools[] = [];
  constructor(private marketplacesolutionsService: MarketplaceSolutionsService) { }

  ngOnInit() {
    this.marketplacesolutionsService.getFilters().subscribe(data =>{
      this.constructedFilter = filterConstruct(data);
    });

    this.marketplacesolutionsService.getTools().subscribe(data =>{
      this.originalTools = data;      
      this.filterTools();
    });
  }
  filterTools(){
    this.filteredTools = [];
    for (let i of this.originalTools){
      if(this.checkedId.length == 0 || i.filtersApplicable.find(value => this.checkedId.includes(value))){
        this.filteredTools.push(i);
      }
    }
  }

  filterChangedEvent(event, id){

    if(event.checked){
      this.checkedId.push(id)
    }
    else{
      this.checkedId.splice(this.checkedId.indexOf(id), 1)
    } 
    
    console.log(event, id);
    
    this.filterTools();

    console.log(this.filteredTools);
    
  }
}
