import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-marketplace-filter',
  templateUrl: './marketplace-filter.component.html',
  styleUrls: ['./marketplace-filter.component.scss']
})
export class MarketplaceFilterComponent implements OnInit {

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
