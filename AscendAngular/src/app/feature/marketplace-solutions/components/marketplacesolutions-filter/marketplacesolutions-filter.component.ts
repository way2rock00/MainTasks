import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-marketplacesolutions-filter',
  templateUrl: './marketplacesolutions-filter.component.html',
  styleUrls: ['./marketplacesolutions-filter.component.scss']
})
export class MarketplacesolutionsFilterComponent implements OnInit {
  @Input() filter: string;
  @Input() type: string;

  @Input() expanded: boolean;
  constructor() { }

  ngOnInit() {
  }
  clicked(){
    this.expanded = !this.expanded;
    event.stopPropagation();
  }
}
